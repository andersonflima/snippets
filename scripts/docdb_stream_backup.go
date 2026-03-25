package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPrefix                      = "docdb/"
	defaultExpectedSizeBytes     int64 = 10 * 1024 * 1024 * 1024
	defaultTargetDurationSeconds       = 60
)

var usageText = `Uso:
  go run scripts/docdb_stream_backup.go <docdb_uri> <bucket>
  go run scripts/docdb_stream_backup.go <docdb_uri> <bucket> <prefix>
  go run scripts/docdb_stream_backup.go <docdb_uri> <bucket> [--prefix docdb/prod] [--num-parallel-collections 16] [--pigz-threads 8] [--compression-level 1] [--expected-size-bytes 10737418240]
  go run scripts/docdb_stream_backup.go <docdb_uri> <bucket> --mongodump-arg --tls --mongodump-arg --tlsCAFile=/path/ca.pem
  go run scripts/docdb_stream_backup.go <docdb_uri> <bucket> --mongodump-arg='--tls' --mongodump-arg='--tlsCAFile=/path/ca.pem'

Exemplos:
  go run scripts/docdb_stream_backup.go 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false' meu-bucket
  go run scripts/docdb_stream_backup.go 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket docdb/prod
  go run scripts/docdb_stream_backup.go 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket --num-parallel-collections 16 --pigz-threads 8 --compression-level 1 --expected-size-bytes 10737418240

  Observação:
    O upload acontece por stream em memória, sem gerar arquivo local no EC2.
    Perfil padrão otimizado para throughput: compressão nível 1 e expected-size de 10 GiB.
    Meta de desempenho: 10 GiB em até 60 segundos.
  A conexão principal é o primeiro argumento posicional.
  Não passe --uri novamente em --mongodump-arg.
`

type backupArgs struct {
	docdbURI           string
	bucket             string
	prefix             string
	numParallel        int
	pigzThreads        int
	compressionLevel   int
	expectedSizeBytes  int64
	extraMongodumpArgs []string
}

type backupMetrics struct {
	rawBytes int64
	duration time.Duration
}

type parsedOptions struct {
	help                   bool
	prefix                 string
	prefixAlias            string
	numParallelCollections int
	pigzThreads            int
	compressionLevel       int
	expectedSizeBytes      int64
	expectedSizeGiB        int64
	extraMongodumpArgs     []string
}

type argsParseError struct {
	msg string
}

func (e argsParseError) Error() string { return e.msg }

var legacyTLSAliases = map[string]string{
	"--tls":                           "--ssl",
	"--tlsAllowInvalidCertificates":   "--sslAllowInvalidCertificates",
	"--tlsAllowInvalidHostnames":      "--sslAllowInvalidHostnames",
	"--tlsCAFile":                     "--sslCAFile",
	"--tlsCRLFile":                    "--sslCRLFile",
	"--tlsCertificateKeyFile":         "--sslPEMKeyFile",
	"--tlsCertificateKeyFilePassword": "--sslPEMKeyPassword",
	"--tlsDisabledProtocols":          "--sslDisabledProtocols",
	"--tlsInsecure":                   "--sslInsecure",
	"--tlsFIPSMode":                   "--sslFIPSMode",
}

var errShowUsage = errors.New("show usage")

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, errShowUsage) {
			fmt.Print(usageText)
			os.Exit(0)
		}

		fmt.Printf("erro: %v\n\n%s\n", err, usageText)
		os.Exit(1)
	}
}

func run(argv []string) error {
	args, err := parseArgs(argv)
	if err != nil {
		return err
	}

	start := time.Now()
	if err := ensureBinaries(); err != nil {
		return err
	}

	key := buildS3Key(args.prefix)
	destination := fmt.Sprintf("s3://%s/%s", args.bucket, key)

	printConfig(args)
	fmt.Printf("destino: %s\n", destination)
	fmt.Printf("alvo: %s em até %ds\n", formatBytesBinary(defaultExpectedSizeBytes), defaultTargetDurationSeconds)

	metrics, err := runPipeline(args, destination)
	metrics.duration = time.Since(start)
	printPerformanceReport(metrics, args.expectedSizeBytes)
	if err != nil {
		return err
	}

	fmt.Printf("backup concluído\ndestino: %s\n", destination)
	return nil
}

func parseArgs(argv []string) (backupArgs, error) {
	parsed := parsedOptions{
		numParallelCollections: defaultNumParallelCollections(),
		pigzThreads:            defaultPigzThreads(),
		compressionLevel:       1,
	}

	positionals, parseErr := parseArgv(argv, &parsed)
	if parseErr != nil {
		return backupArgs{}, parseErr
	}

	docdbURI, err := normalizeNonEmpty(positionals[0], "docdb_uri")
	if err != nil {
		return backupArgs{}, argsParseError{msg: err.Error()}
	}

	docdbURI, err = validateDocdbURI(docdbURI)
	if err != nil {
		return backupArgs{}, argsParseError{msg: err.Error()}
	}

	bucket, err := normalizeNonEmpty(positionals[1], "bucket")
	if err != nil {
		return backupArgs{}, argsParseError{msg: err.Error()}
	}

	prefixFromPositional := ""
	if len(positionals) == 3 {
		prefixFromPositional = positionals[2]
	}

	if prefixFromPositional != "" && (parsed.prefix != "" || parsed.prefixAlias != "") {
		return backupArgs{}, argsParseError{msg: "use prefix posicional ou --prefix, não os dois"}
	}

	if parsed.prefix == "" {
		parsed.prefix = parsed.prefixAlias
	}

	prefix, err := normalizePrefix(resolvePrefixSource(prefixFromPositional, parsed.prefix))
	if err != nil {
		return backupArgs{}, argsParseError{msg: err.Error()}
	}

	numParallel, err := resolvePositiveInt(parsed.numParallelCollections, "num-parallel-collections")
	if err != nil {
		return backupArgs{}, argsParseError{msg: err.Error()}
	}

	pigzThreads, err := resolvePositiveInt(parsed.pigzThreads, "pigz-threads")
	if err != nil {
		return backupArgs{}, argsParseError{msg: err.Error()}
	}

	if parsed.compressionLevel < 1 || parsed.compressionLevel > 9 {
		return backupArgs{}, argsParseError{msg: "compression-level precisa estar entre 1 e 9"}
	}

	expectedSizeBytes, err := resolveExpectedSizeBytes(parsed.expectedSizeBytes, parsed.expectedSizeGiB)
	if err != nil {
		return backupArgs{}, argsParseError{msg: err.Error()}
	}

	extraMongodumpArgs := translateLegacyTLSArgs(parsed.extraMongodumpArgs)
	extraMongodumpArgs, err = validateMongodumpConnectionArgs(extraMongodumpArgs)
	if err != nil {
		return backupArgs{}, argsParseError{msg: err.Error()}
	}

	return backupArgs{
		docdbURI:           docdbURI,
		bucket:             bucket,
		prefix:             prefix,
		numParallel:        numParallel,
		pigzThreads:        pigzThreads,
		compressionLevel:   parsed.compressionLevel,
		expectedSizeBytes:  expectedSizeBytes,
		extraMongodumpArgs: extraMongodumpArgs,
	}, nil
}

func parseArgv(argv []string, parsed *parsedOptions) ([]string, error) {
	positionals := make([]string, 0, 2)

	for i := 0; i < len(argv); i++ {
		token := argv[i]
		if token == "--" {
			positionals = append(positionals, argv[i+1:]...)
			break
		}

		if token == "--help" || token == "-h" {
			return nil, errShowUsage
		}

		if strings.HasPrefix(token, "--") {
			name, value, hasValue, err := parseLongOption(token, argv, i)
			if err != nil {
				return nil, err
			}

			if hasValue {
				switch name {
				case "prefix":
					parsed.prefix = value
				case "num-parallel-collections":
					numParallelCollections, err := strconv.Atoi(value)
					if err != nil {
						return nil, argsParseError{msg: "num-parallel-collections precisa ser inteiro"}
					}
					parsed.numParallelCollections = numParallelCollections
				case "pigz-threads":
					pigzThreads, err := strconv.Atoi(value)
					if err != nil {
						return nil, argsParseError{msg: "pigz-threads precisa ser inteiro"}
					}
					parsed.pigzThreads = pigzThreads
				case "compression-level":
					compressionLevel, err := strconv.Atoi(value)
					if err != nil {
						return nil, argsParseError{msg: "compression-level precisa ser inteiro"}
					}
					parsed.compressionLevel = compressionLevel
				case "expected-size-bytes":
					expectedSizeBytes, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, argsParseError{msg: "expected-size-bytes precisa ser inteiro"}
					}
					parsed.expectedSizeBytes = expectedSizeBytes
				case "expected-size-gib":
					expectedSizeGiB, err := strconv.ParseInt(value, 10, 64)
					if err != nil {
						return nil, argsParseError{msg: "expected-size-gib precisa ser inteiro"}
					}
					parsed.expectedSizeGiB = expectedSizeGiB
				case "mongodump-arg":
					parsed.extraMongodumpArgs = append(parsed.extraMongodumpArgs, value)
				default:
					return nil, argsParseError{msg: fmt.Sprintf("opção inválida: --%s", name)}
				}

				i++
				continue
			}

			switch name {
			case "help", "h":
				return nil, errShowUsage
			case "mongodump-arg", "p", "prefix", "num-parallel-collections", "pigz-threads", "compression-level", "expected-size-bytes", "expected-size-gib":
				return nil, argsParseError{msg: fmt.Sprintf("opção --%s requer valor", name)}
			default:
				return nil, argsParseError{msg: fmt.Sprintf("opção inválida: --%s", name)}
			}
		}

		if token == "-" || !strings.HasPrefix(token, "-") {
			positionals = append(positionals, token)
			continue
		}

		switch {
		case token == "-p":
			if i+1 >= len(argv) {
				return nil, argsParseError{msg: "opção -p requer valor"}
			}
			parsed.prefixAlias = argv[i+1]
			if strings.TrimSpace(parsed.prefixAlias) == "" {
				return nil, argsParseError{msg: "opção -p não pode ser vazio"}
			}
			i++
			continue
		default:
			return nil, argsParseError{msg: fmt.Sprintf("opção inválida: %s", token)}
		}
	}

	if len(positionals) < 2 || len(positionals) > 3 {
		return nil, argsParseError{msg: "argumentos inválidos"}
	}

	return positionals, nil
}

func parseLongOption(token string, argv []string, index int) (string, string, bool, error) {
	option := strings.TrimPrefix(token, "--")
	if option == "" {
		return "", "", false, argsParseError{msg: "opção inválida: --"}
	}

	if strings.Contains(option, "=") {
		split := strings.SplitN(option, "=", 2)
		return split[0], split[1], true, nil
	}

	if index+1 >= len(argv) {
		return option, "", false, nil
	}

	return option, argv[index+1], true, nil
}

func translateLegacyTLSArgs(args []string) []string {
	helpOutput, err := getMongodumpHelp()
	if err != nil {
		return appendQuietIfSupported(args, "")
	}

	supportsTLS := isFlagInHelp(helpOutput, "--tls")
	supportsSSL := isFlagInHelp(helpOutput, "--ssl")

	translated := args
	if !supportsTLS && supportsSSL {
		translated = make([]string, 0, len(args))
		for _, arg := range args {
			translated = append(translated, translateLegacyTLSArg(arg))
		}
	}

	return appendQuietIfSupported(translated, helpOutput)
}

func appendQuietIfSupported(args []string, helpText string) []string {
	for _, arg := range args {
		if arg == "--quiet" || strings.HasPrefix(arg, "--quiet=") {
			return args
		}
	}

	if helpText == "" {
		return args
	}

	if isFlagInHelp(helpText, "--quiet") {
		return append(args, "--quiet")
	}

	return args
}

func getMongodumpHelp() (string, error) {
	output, err := exec.Command("mongodump", "--help").CombinedOutput()
	if err != nil {
		return "", err
	}
	return string(output), nil
}

func isFlagInHelp(helpText, flagName string) bool {
	candidates := []string{
		" " + flagName + " ",
		"\n" + flagName + " ",
		" " + flagName + "=",
		"\n" + flagName + "=",
	}

	for _, candidate := range candidates {
		if strings.Contains(helpText, candidate) {
			return true
		}
	}
	return false
}

func translateLegacyTLSArg(arg string) string {
	name, value, hasValue := splitArgWithValue(arg)
	replacement, ok := legacyTLSAliases[name]
	if !ok {
		return arg
	}

	if !hasValue {
		return replacement
	}
	return replacement + "=" + value
}

func splitArgWithValue(arg string) (string, string, bool) {
	parts := strings.SplitN(arg, "=", 2)
	if len(parts) == 1 {
		return arg, "", false
	}
	return parts[0], parts[1], true
}

func validateMongodumpConnectionArgs(args []string) ([]string, error) {
	for _, arg := range args {
		if isUriConnectionArg(arg) {
			return nil, fmt.Errorf(
				"não use string de conexão em --mongodump-arg: %s\nA URI já é passada como primeiro argumento do script e enviada via --uri",
				arg,
			)
		}
	}
	return args, nil
}

func isUriConnectionArg(value string) bool {
	normalized := strings.TrimSpace(value)
	return normalized == "--uri" ||
		strings.HasPrefix(normalized, "--uri=") ||
		strings.HasPrefix(normalized, "mongodb://") ||
		strings.HasPrefix(normalized, "mongodb+srv://")
}

func normalizeNonEmpty(value, label string) (string, error) {
	normalized := strings.TrimSpace(value)
	if normalized == "" {
		return "", fmt.Errorf("%s não pode ser vazio", label)
	}
	return normalized, nil
}

func validateDocdbURI(uri string) (string, error) {
	normalized := strings.TrimSpace(uri)
	switch {
	case strings.HasPrefix(normalized, "mongodb://"):
		return normalized, nil
	case strings.HasPrefix(normalized, "mongodb+srv://"):
		return "", fmt.Errorf(
			"documentdb requer mongodb://, mas a URI recebida usa mongodb+srv://: %s",
			normalized,
		)
	case strings.Contains(normalized, "://"):
		preview := normalized
		if len(preview) > 80 {
			preview = preview[:80] + "..."
		}
		return "", fmt.Errorf("documentdb URI com formato inválido; esperado mongodb://..., recebido: %s", preview)
	default:
		preview := normalized
		if len(preview) > 80 {
			preview = preview[:80] + "..."
		}
		return "", fmt.Errorf("documentdb URI inválida (esperado mongodb://): %s", preview)
	}
}

func resolvePrefixSource(positionalPrefix, optionPrefix string) string {
	if positionalPrefix != "" {
		return positionalPrefix
	}
	return optionPrefix
}

func normalizePrefix(value string) (string, error) {
	normalized := strings.TrimSpace(value)
	if normalized == "" {
		return defaultPrefix, nil
	}

	sanitized := strings.TrimLeft(normalized, "/")
	sanitized = strings.Join(strings.FieldsFunc(sanitized, func(r rune) bool { return r == '/' }), "/")
	if sanitized == "" {
		return defaultPrefix, nil
	}

	if strings.HasSuffix(sanitized, "/") {
		return sanitized, nil
	}
	return sanitized + "/", nil
}

func resolvePositiveInt(value int, label string) (int, error) {
	if value <= 0 {
		return 0, fmt.Errorf("%s precisa ser inteiro positivo", label)
	}
	return value, nil
}

func resolveExpectedSizeBytes(sizeBytes, sizeGiB int64) (int64, error) {
	if sizeBytes != 0 && sizeGiB != 0 {
		return 0, fmt.Errorf("use apenas expected-size-bytes ou expected-size-gib")
	}

	if sizeBytes != 0 {
		if sizeBytes <= 0 {
			return 0, fmt.Errorf("expected-size-bytes precisa ser inteiro positivo")
		}
		return sizeBytes, nil
	}

	if sizeGiB != 0 {
		if sizeGiB <= 0 {
			return 0, fmt.Errorf("expected-size-gib precisa ser inteiro positivo")
		}
		return sizeGiB * 1024 * 1024 * 1024, nil
	}

	return defaultExpectedSizeBytes, nil
}

func ensureBinaries() error {
	for _, binary := range []string{"bash", "mongodump", "pigz", "aws"} {
		if _, err := exec.LookPath(binary); err != nil {
			return fmt.Errorf("binário obrigatório não encontrado no PATH: %s", binary)
		}
	}
	return nil
}

func buildS3Key(prefix string) string {
	timestamp := time.Now().UTC().Format(time.RFC3339Nano)
	timestamp = strings.ReplaceAll(timestamp, ":", "")
	timestamp = strings.ReplaceAll(timestamp, "-", "")
	timestamp = strings.ReplaceAll(timestamp, ".", "")
	return fmt.Sprintf("%sdocdb-backup-%s.archive.gz", prefix, timestamp)
}

func runPipeline(args backupArgs, destination string) (backupMetrics, error) {
	metrics := backupMetrics{}
	ctx := context.Background()
	stopSpinner, doneSpinner := startProgressSpinner()
	defer stopProgressSpinner(stopSpinner, doneSpinner)

	mongodumpArgs := make([]string, 0, len(args.extraMongodumpArgs)+3)
	mongodumpArgs = append(mongodumpArgs, "--uri", args.docdbURI, "--archive")
	if supportsNumParallelCollections() {
		mongodumpArgs = append(mongodumpArgs, "--numParallelCollections", strconv.Itoa(args.numParallel))
	}
	mongodumpArgs = append(mongodumpArgs, args.extraMongodumpArgs...)

	pigzArgs := []string{"-c", fmt.Sprintf("-%d", args.compressionLevel), "-p", strconv.Itoa(args.pigzThreads)}
	awsArgs := []string{"s3", "cp", "-", destination, "--no-progress", "--only-show-errors", "--expected-size", strconv.FormatInt(args.expectedSizeBytes, 10)}

	mongodumpCmd := exec.CommandContext(ctx, "mongodump", mongodumpArgs...)
	pigzCmd := exec.CommandContext(ctx, "pigz", pigzArgs...)
	awsCmd := exec.CommandContext(ctx, "aws", awsArgs...)
	var (
		mongodumpErr bytes.Buffer
		pigzErr      bytes.Buffer
		awsErr       bytes.Buffer
	)

	dumpOut, err := mongodumpCmd.StdoutPipe()
	if err != nil {
		return metrics, err
	}

	pigzIn, err := pigzCmd.StdinPipe()
	if err != nil {
		return metrics, err
	}

	pigzOut, err := pigzCmd.StdoutPipe()
	if err != nil {
		return metrics, err
	}

	mongodumpCmd.Stderr = &mongodumpErr
	pigzCmd.Stderr = &pigzErr
	awsCmd.Stderr = &awsErr
	awsCmd.Stdin = pigzOut

	if err := pigzCmd.Start(); err != nil {
		return metrics, runPipelineError("falha ao iniciar pigz", err, formatCommandForLog("pigz", pigzArgs), &pigzErr, &awsErr)
	}

	if err := awsCmd.Start(); err != nil {
		_ = pigzCmd.Process.Kill()
		return metrics, runPipelineError("falha ao iniciar aws", err, formatCommandForLog("aws", awsArgs), &pigzErr, &awsErr)
	}

	if err := mongodumpCmd.Start(); err != nil {
		_ = pigzCmd.Process.Kill()
		_ = awsCmd.Process.Kill()
		return metrics, runPipelineError("falha ao iniciar mongodump", err, formatCommandForLog("mongodump", mongodumpArgs), &mongodumpErr, &pigzErr, &awsErr)
	}

	counter := &byteCounter{reader: dumpOut}
	copyDone := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(pigzIn, counter)
		_ = pigzIn.Close()
		copyDone <- copyErr
	}()

	if err := mongodumpCmd.Wait(); err != nil {
		_ = <-copyDone
		_ = pigzCmd.Process.Kill()
		_ = awsCmd.Process.Kill()
		metrics.rawBytes = counter.bytes
		return metrics, runPipelineError("mongodump falhou", err, formatCommandForLog("mongodump", mongodumpArgs), &mongodumpErr, &pigzErr, &awsErr)
	}

	if copyErr := <-copyDone; copyErr != nil {
		_ = pigzCmd.Process.Kill()
		_ = awsCmd.Process.Kill()
		metrics.rawBytes = counter.bytes
		return metrics, runPipelineError("falha ao encaminhar fluxo entre mongodump e pigz", copyErr, "", &pigzErr, &awsErr)
	}

	if err := pigzCmd.Wait(); err != nil {
		_ = awsCmd.Process.Kill()
		metrics.rawBytes = counter.bytes
		return metrics, runPipelineError("pigz falhou", err, formatCommandForLog("pigz", pigzArgs), &pigzErr, &awsErr, &mongodumpErr)
	}

	if err := awsCmd.Wait(); err != nil {
		metrics.rawBytes = counter.bytes
		return metrics, runPipelineError("aws falhou", err, formatCommandForLog("aws", awsArgs), &awsErr, &pigzErr, &mongodumpErr)
	}

	metrics.rawBytes = counter.bytes
	return metrics, nil
}

func printConfig(args backupArgs) {
	fmt.Printf(
		"config: numParallelCollections=%d pigz_threads=%d compression_level=%d expected_size=%s\n",
		args.numParallel,
		args.pigzThreads,
		args.compressionLevel,
		formatBytesBinary(args.expectedSizeBytes),
	)
}

func printPerformanceReport(metrics backupMetrics, expectedBytes int64) {
	targetDuration := time.Duration(defaultTargetDurationSeconds) * time.Second
	fmt.Printf("tempo total: %s\n", formatDuration(metrics.duration))

	if metrics.rawBytes > 0 {
		seconds := metrics.duration.Seconds()
		if seconds <= 0 {
			seconds = 1
		}
		mbPerSecond := float64(metrics.rawBytes) / 1024.0 / 1024.0 / seconds
		fmt.Printf(
			"volume processado: %s (%0.2f MiB/s)\n",
			formatBytesBinary(metrics.rawBytes),
			mbPerSecond,
		)
	} else {
		fmt.Printf("volume processado: sem bytes (não foi possível mensurar)\n")
	}

	targetMiBPerSecond := float64(expectedBytes) / 1024.0 / 1024.0 / targetDuration.Seconds()
	targetGiBPerMinute := float64(expectedBytes) / 1024.0 / 1024.0 / 1024.0 / targetDuration.Minutes()
	resultStatus := "não atingido"
	if metrics.duration <= targetDuration {
		resultStatus = "atingido"
	}

	fmt.Printf(
		"meta de throughput: %0.2f MiB/s (ou %0.2f GiB/min) | resultado: %s\n",
		targetMiBPerSecond,
		targetGiBPerMinute,
		resultStatus,
	)
}

func formatBytesBinary(size int64) string {
	const unit = 1024.0
	if size <= 0 {
		return "0 B"
	}

	divisions := []string{"B", "KiB", "MiB", "GiB", "TiB"}
	value := float64(size)
	for _, label := range divisions {
		if value < unit {
			return fmt.Sprintf("%.2f %s", value, label)
		}
		value = value / unit
	}

	return fmt.Sprintf("%.2f TiB", value)
}

func formatDuration(d time.Duration) string {
	seconds := int64(d.Seconds())
	minutes := seconds / 60
	remainingSeconds := seconds % 60
	if minutes > 0 {
		return fmt.Sprintf("%dm%02ds", minutes, remainingSeconds)
	}
	return fmt.Sprintf("%ds", remainingSeconds)
}

func runPipelineError(commandName string, commandErr error, commandLine string, buffers ...*bytes.Buffer) error {
	parts := make([]string, 0, len(buffers)+1)
	parts = append(parts, commandName)
	if commandLine != "" {
		parts = append(parts, fmt.Sprintf("comando: %s", commandLine))
	}
	for _, buffer := range buffers {
		text := strings.TrimSpace(buffer.String())
		if text != "" {
			parts = append(parts, text)
		}
	}

	if len(parts) == 0 {
		return fmt.Errorf("%s: %w", commandName, commandErr)
	}

	return fmt.Errorf("%s: %w\n%s", commandName, commandErr, strings.Join(parts, "\n"))
}

func formatCommandForLog(command string, args []string) string {
	sanitizedArgs := make([]string, 0, len(args))
	for _, arg := range maskSensitiveArgs(command, args) {
		sanitizedArgs = append(sanitizedArgs, strconv.Quote(arg))
	}
	return strings.Join(append([]string{command}, sanitizedArgs...), " ")
}

func maskSensitiveArgs(command string, args []string) []string {
	if command != "mongodump" {
		return args
	}

	masked := make([]string, len(args))
	copy(masked, args)

	for index := 0; index < len(masked); index++ {
		switch masked[index] {
		case "--uri":
			if index+1 < len(masked) {
				masked[index+1] = maskMongoUri(masked[index+1])
			}
		default:
			if strings.HasPrefix(masked[index], "--uri=") {
				masked[index] = "--uri=" + maskMongoUri(strings.TrimPrefix(masked[index], "--uri="))
			}
		}
	}

	return masked
}

func maskMongoUri(uri string) string {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "mongodb://***:***@***"
	}

	if parsed.User == nil {
		return uri
	}

	user := parsed.User.Username()
	if user == "" {
		return uri
	}

	password, hasPassword := parsed.User.Password()
	if !hasPassword {
		parsed.User = url.User(user)
		return parsed.String()
	}
	if password == "" {
		parsed.User = url.UserPassword(user, "***")
		return parsed.String()
	}

	parsed.User = url.UserPassword(user, "***")
	return parsed.String()
}

func supportsNumParallelCollections() bool {
	helpOutput, err := getMongodumpHelp()
	if err != nil {
		return true
	}
	return isFlagInHelp(helpOutput, "--numParallelCollections")
}

func startProgressSpinner() (chan struct{}, chan struct{}) {
	stop := make(chan struct{})
	done := make(chan struct{})

	go func() {
		defer close(done)

		frames := []string{"|", "/", "-", "\\"}
		start := time.Now()
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		for i := 0; ; i++ {
			select {
			case <-ticker.C:
				fmt.Printf("\rbackup em andamento %s (%s)", frames[i%len(frames)], time.Since(start).Truncate(time.Second))
			case <-stop:
				fmt.Print("\r\033[2K")
				return
			}
		}
	}()

	return stop, done
}

func stopProgressSpinner(stop chan struct{}, done chan struct{}) {
	close(stop)
	<-done
}

func defaultNumParallelCollections() int {
	candidate := runtime.NumCPU()
	if candidate < 16 {
		return 16
	}
	if candidate > 32 {
		return 32
	}
	return candidate
}

func defaultPigzThreads() int {
	numCPU := runtime.NumCPU()
	if numCPU < 8 {
		return 8
	}
	if numCPU > 16 {
		return 16
	}
	return numCPU
}

type byteCounter struct {
	reader io.Reader
	bytes  int64
}

func (counter *byteCounter) Read(p []byte) (int, error) {
	n, err := counter.reader.Read(p)
	counter.bytes += int64(n)
	return n, err
}
