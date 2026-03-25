package main

import (
	"context"
	"errors"
	"flag"
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

type stringSliceFlag []string

type argsParseError struct {
	msg string
}

func (e argsParseError) Error() string { return e.msg }

func (f *stringSliceFlag) String() string {
	return strings.Join(*f, ",")
}

func (f *stringSliceFlag) Set(value string) error {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return nil
	}
	*f = append(*f, trimmed)
	return nil
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
	flagSet := flag.NewFlagSet("docdb-stream-backup", flag.ContinueOnError)
	flagSet.SetOutput(io.Discard)

	type options struct {
		help                   bool
		prefix                 string
		prefixAlias            string
		numParallelCollections int
		pigzThreads            int
		compressionLevel       int
		expectedSizeBytes      int64
		expectedSizeGiB        int64
		extraMongodumpArgs     stringSliceFlag
	}

	parsed := options{
		numParallelCollections: defaultNumParallelCollections(),
		pigzThreads:            defaultPigzThreads(),
		compressionLevel:       1,
	}

	flagSet.BoolVar(&parsed.help, "help", false, "show help")
	flagSet.BoolVar(&parsed.help, "h", false, "show help")
	flagSet.StringVar(&parsed.prefix, "prefix", "", "s3 prefix")
	flagSet.StringVar(&parsed.prefixAlias, "p", "", "s3 prefix")
	flagSet.IntVar(&parsed.numParallelCollections, "num-parallel-collections", parsed.numParallelCollections, "mongodump parallel collections")
	flagSet.IntVar(&parsed.pigzThreads, "pigz-threads", parsed.pigzThreads, "pigz threads")
	flagSet.IntVar(&parsed.compressionLevel, "compression-level", parsed.compressionLevel, "compression level")
	flagSet.Int64Var(&parsed.expectedSizeBytes, "expected-size-bytes", 0, "expected size bytes")
	flagSet.Int64Var(&parsed.expectedSizeGiB, "expected-size-gib", 0, "expected size gib")
	flagSet.Var(&parsed.extraMongodumpArgs, "mongodump-arg", "additional mongodump arg")

	if err := flagSet.Parse(argv); err != nil {
		return backupArgs{}, err
	}

	if parsed.help {
		return backupArgs{}, errShowUsage
	}

	positionals := flagSet.Args()
	if len(positionals) < 2 || len(positionals) > 3 {
		return backupArgs{}, argsParseError{msg: "argumentos inválidos"}
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

	if prefixFromPositional != "" && parsed.prefix != "" {
		return backupArgs{}, argsParseError{msg: "use prefix posicional ou --prefix, não os dois"}
	}

	if parsed.prefix != "" && parsed.prefixAlias != "" && parsed.prefix != parsed.prefixAlias {
		return backupArgs{}, argsParseError{msg: "opções --prefix e -p divergem"}
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

	return backupArgs{
		docdbURI:           docdbURI,
		bucket:             bucket,
		prefix:             prefix,
		numParallel:        numParallel,
		pigzThreads:        pigzThreads,
		compressionLevel:   parsed.compressionLevel,
		expectedSizeBytes:  expectedSizeBytes,
		extraMongodumpArgs: parsed.extraMongodumpArgs,
	}, nil
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
