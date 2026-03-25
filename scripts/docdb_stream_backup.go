package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	defaultPrefix                  = "docdb/"
	defaultExpectedSizeBytes int64 = 10 * 1024 * 1024 * 1024
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

		fmt.Fprintf(os.Stderr, "erro: %v\n\n%s\n", err, usageText)
		os.Exit(1)
	}
}

func run(argv []string) error {
	args, err := parseArgs(argv)
	if err != nil {
		return err
	}

	if err := ensureBinaries(); err != nil {
		return err
	}

	key := buildS3Key(args.prefix)
	destination := fmt.Sprintf("s3://%s/%s", args.bucket, key)

	fmt.Printf("destino: %s\n", destination)

	if err := runPipeline(args, destination); err != nil {
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
		return args
	}

	supportsTLS := isFlagInHelp(helpOutput, "--tls")
	supportsSSL := isFlagInHelp(helpOutput, "--ssl")
	if supportsTLS || !supportsSSL {
		return args
	}

	translated := make([]string, 0, len(args))
	for _, arg := range args {
		translated = append(translated, translateLegacyTLSArg(arg))
	}

	return translated
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

func normalizeNonEmpty(value, label string) (string, error) {
	normalized := strings.TrimSpace(value)
	if normalized == "" {
		return "", fmt.Errorf("%s não pode ser vazio", label)
	}
	return normalized, nil
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

func runPipeline(args backupArgs, destination string) error {
	ctx := context.Background()

	mongodumpArgs := make([]string, 0, len(args.extraMongodumpArgs)+3)
	mongodumpArgs = append(mongodumpArgs, "--uri", args.docdbURI, "--archive", "--numParallelCollections", strconv.Itoa(args.numParallel))
	mongodumpArgs = append(mongodumpArgs, args.extraMongodumpArgs...)

	pigzArgs := []string{"-c", fmt.Sprintf("-%d", args.compressionLevel), "-p", strconv.Itoa(args.pigzThreads)}
	awsArgs := []string{"s3", "cp", "-", destination, "--no-progress", "--only-show-errors", "--expected-size", strconv.FormatInt(args.expectedSizeBytes, 10)}

	mongodumpCmd := exec.CommandContext(ctx, "mongodump", mongodumpArgs...)
	pigzCmd := exec.CommandContext(ctx, "pigz", pigzArgs...)
	awsCmd := exec.CommandContext(ctx, "aws", awsArgs...)

	dumpOut, err := mongodumpCmd.StdoutPipe()
	if err != nil {
		return err
	}

	pigzIn, err := pigzCmd.StdinPipe()
	if err != nil {
		return err
	}

	pigzOut, err := pigzCmd.StdoutPipe()
	if err != nil {
		return err
	}

	mongodumpCmd.Stderr = os.Stderr
	pigzCmd.Stderr = os.Stderr
	awsCmd.Stderr = os.Stderr
	awsCmd.Stdin = pigzOut

	if err := pigzCmd.Start(); err != nil {
		return err
	}

	if err := awsCmd.Start(); err != nil {
		_ = pigzCmd.Process.Kill()
		return err
	}

	if err := mongodumpCmd.Start(); err != nil {
		_ = pigzCmd.Process.Kill()
		_ = awsCmd.Process.Kill()
		return err
	}

	copyDone := make(chan error, 1)
	go func() {
		_, copyErr := io.Copy(pigzIn, dumpOut)
		_ = pigzIn.Close()
		copyDone <- copyErr
	}()

	if err := mongodumpCmd.Wait(); err != nil {
		_ = <-copyDone
		_ = pigzCmd.Process.Kill()
		_ = awsCmd.Process.Kill()
		return err
	}

	if copyErr := <-copyDone; copyErr != nil {
		_ = pigzCmd.Process.Kill()
		_ = awsCmd.Process.Kill()
		return copyErr
	}

	if err := pigzCmd.Wait(); err != nil {
		_ = awsCmd.Process.Kill()
		return err
	}

	if err := awsCmd.Wait(); err != nil {
		return err
	}

	return nil
}

func defaultNumParallelCollections() int {
	candidate := runtime.NumCPU() * 2
	if candidate < 8 {
		candidate = 8
	}
	if candidate > 32 {
		candidate = 32
	}
	return candidate
}

func defaultPigzThreads() int {
	numCPU := runtime.NumCPU()
	if numCPU < 1 {
		return 1
	}
	return numCPU
}
