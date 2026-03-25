#!/usr/bin/env elixir


defmodule DocdbStreamBackup do
  @default_prefix "docdb/"
  @default_expected_size_bytes 10 * 1024 * 1024 * 1024
  @default_target_duration_seconds 60

  @usage """
Uso:
  elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket>
  elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> <prefix>
  elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> [--prefix docdb/prod] [--num-parallel-collections 16] [--pigz-threads 8] [--compression-level 1] [--expected-size-bytes 10737418240]

Exemplos:
  elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false' meu-bucket
  elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket docdb/prod
  elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket --num-parallel-collections 16 --pigz-threads 8 --compression-level 1 --expected-size-bytes 10737418240
  elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket --mongodump-arg --tls --mongodump-arg --tlsCAFile=/path/ca.pem
  elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket --mongodump-arg='--tls' --mongodump-arg='--tlsCAFile=/path/ca.pem'

Observação:
  O upload acontece por stream em memória, sem gerar arquivo local no EC2.
  Perfil padrão otimizado para throughput: compressão nível 1 e expected-size de 10 GiB.
  Meta de desempenho: 10 GiB em até 60 segundos.
  A string de conexão principal é o primeiro argumento posicional.
  Não passe --uri novamente em --mongodump-arg.
"""

  def main(argv) do
    case parse_args(argv) do
      {:help, message} ->
        IO.puts(message)
        :ok

      {:ok, args} ->
        with :ok <- ensure_binary("bash"),
             :ok <- ensure_binary("mongodump"),
             :ok <- ensure_binary("pigz"),
             :ok <- ensure_binary("aws"),
             {:ok, key} <- build_s3_key(args.prefix),
             {:ok, metrics} <- run_pipeline(args, key) do
          print_performance_report(metrics, args.expected_size_bytes)
          IO.puts("backup concluído")
          IO.puts("destino: s3://#{args.bucket}/#{key}")
          :ok
        else
          {:error, message, metrics} ->
            print_performance_report(metrics, args.expected_size_bytes)
            IO.puts("erro: #{message}")
            IO.puts(@usage)
            System.halt(1)
          {:error, message} ->
            IO.puts("erro: #{message}")
            IO.puts(@usage)
            System.halt(1)
        end

      {:error, message} ->
        IO.puts("erro: #{message}")
        IO.puts(@usage)
        System.halt(1)
    end
  end

  defp parse_args(argv) do
    with {:ok, normalized_argv} <- normalize_mongodump_arg_syntax(argv) do
      do_parse_args(normalized_argv)
    end
  end

  defp do_parse_args(argv) do
    {options, positional_args, invalid_options} =
      OptionParser.parse(argv,
        strict: [
          help: :boolean,
          prefix: :string,
          num_parallel_collections: :integer,
          pigz_threads: :integer,
          compression_level: :integer,
          expected_size_bytes: :integer,
          expected_size_gib: :integer,
          mongodump_arg: :keep
        ],
        aliases: [
          h: :help,
          p: :prefix
        ]
      )

    cond do
      options[:help] ->
        {:help, @usage}

      invalid_options != [] ->
        invalid_message = invalid_options |> Enum.map_join(", ", &format_invalid_option/1)
        {:error, "opções inválidas: #{invalid_message}"}

      true ->
        with {:ok, positional} <- parse_positional_args(positional_args),
             {:ok, normalized_uri} <- normalize_non_empty(positional.uri, "docdb_uri"),
             {:ok, validated_uri} <- validate_docdb_uri(normalized_uri),
             {:ok, normalized_bucket} <- normalize_non_empty(positional.bucket, "bucket"),
             {:ok, normalized_prefix} <- resolve_prefix(positional.prefix, options[:prefix]),
             {:ok, num_parallel_collections} <-
               resolve_positive_integer(
                 options[:num_parallel_collections],
                 default_num_parallel_collections(),
                 "num_parallel_collections"
               ),
             {:ok, pigz_threads} <-
               resolve_positive_integer(options[:pigz_threads], default_pigz_threads(), "pigz_threads"),
             {:ok, compression_level} <- resolve_compression_level(options[:compression_level]),
             {:ok, expected_size_bytes} <- resolve_expected_size_bytes(options),
             {:ok, extra_mongodump_args} <- resolve_mongodump_args(options) do
          {:ok,
           %{
             uri: validated_uri,
             bucket: normalized_bucket,
             prefix: normalized_prefix,
             num_parallel_collections: num_parallel_collections,
             pigz_threads: pigz_threads,
             compression_level: compression_level,
             expected_size_bytes: expected_size_bytes,
             extra_mongodump_args: extra_mongodump_args
           }}
        end
    end
  end

  defp normalize_mongodump_arg_syntax(argv) do
    normalize_mongodump_arg_syntax(argv, [])
  end

  defp normalize_mongodump_arg_syntax([], acc), do: {:ok, Enum.reverse(acc)}

  defp normalize_mongodump_arg_syntax(["--mongodump-arg" | tail], acc) do
    case tail do
      [] ->
        {:error, "opção --mongodump-arg requer valor. Ex.: --mongodump-arg=--tls ou --mongodump-arg --tls"}

      [value | rest] ->
        normalize_mongodump_arg_syntax(rest, ["--mongodump-arg=#{value}" | acc])
    end
  end

  defp normalize_mongodump_arg_syntax([arg | tail], acc),
    do: normalize_mongodump_arg_syntax(tail, [arg | acc])

  defp parse_mongodump_option_compatibility(args) do
    with {:ok, help_text} <- fetch_mongodump_help() do
      supports_tls? = flag_supported?(help_text, "--tls")
      supports_ssl? = flag_supported?(help_text, "--ssl")
      supports_quiet? = flag_supported?(help_text, "--quiet")

      translated_args =
        if supports_tls? or !supports_ssl? do
          args
        else
          Enum.map(args, &translate_legacy_tls_to_ssl_arg/1)
        end

      if supports_quiet? do
        append_quiet_arg(translated_args)
      else
        translated_args
      end
    else
      _ -> args
    end
  end

  defp append_quiet_arg(args) do
    has_quiet_arg? =
      args
      |> Enum.any?(fn arg ->
        normalized = String.trim(arg)
        normalized == "--quiet" || String.starts_with?(normalized, "--quiet=")
      end)

    if has_quiet_arg? do
      args
    else
      args ++ ["--quiet"]
    end
  end

  defp fetch_mongodump_help do
    try do
      case System.cmd("mongodump", ["--help"], stderr_to_stdout: true) do
        {text, 0} -> {:ok, text}
        _ -> {:error, "não foi possível consultar --help do mongodump"}
      end
    rescue
      _ ->
        {:error, "não foi possível consultar --help do mongodump"}
    end
  end

  defp flag_supported?(text, flag) do
    regex = ~r/(^|\s)#{Regex.escape(flag)}(\s|=|,)/
    String.contains?(text, flag) && Regex.match?(regex, text)
  end

  @legacy_tls_to_ssl %{
    "--tls" => "--ssl",
    "--tlsAllowInvalidCertificates" => "--sslAllowInvalidCertificates",
    "--tlsAllowInvalidHostnames" => "--sslAllowInvalidHostnames",
    "--tlsCAFile" => "--sslCAFile",
    "--tlsCRLFile" => "--sslCRLFile",
    "--tlsCertificateKeyFile" => "--sslPEMKeyFile",
    "--tlsCertificateKeyFilePassword" => "--sslPEMKeyPassword",
    "--tlsDisabledProtocols" => "--sslDisabledProtocols",
    "--tlsInsecure" => "--sslInsecure",
    "--tlsFIPSMode" => "--sslFIPSMode"
  }

  defp translate_legacy_tls_to_ssl_arg(arg) do
    {flag, value} = split_arg_with_value(arg)

    case Map.get(@legacy_tls_to_ssl, flag) do
      nil ->
        arg

      replacement ->
        if value == "" do
          replacement
        else
          "#{replacement}=#{value}"
        end
    end
  end

  defp split_arg_with_value(arg) do
    case String.split(arg, "=", parts: 2) do
      [flag, value] -> {flag, value}
      [flag] -> {flag, ""}
    end
  end

  defp parse_positional_args([uri, bucket]) do
    {:ok, %{uri: uri, bucket: bucket, prefix: nil}}
  end

  defp parse_positional_args([uri, bucket, prefix]) do
    {:ok, %{uri: uri, bucket: bucket, prefix: prefix}}
  end

  defp parse_positional_args(_), do: {:error, "argumentos inválidos"}

  defp format_invalid_option({option, nil}), do: to_string(option)
  defp format_invalid_option({option, value}), do: "#{option}=#{inspect(value)}"

  defp resolve_prefix(positional_prefix, option_prefix)

  defp resolve_prefix(nil, nil), do: {:ok, @default_prefix}
  defp resolve_prefix(nil, prefix), do: normalize_prefix(prefix)
  defp resolve_prefix(prefix, nil), do: normalize_prefix(prefix)

  defp resolve_prefix(_positional_prefix, _option_prefix),
    do: {:error, "use prefix posicional ou --prefix, não os dois"}

  defp normalize_non_empty(value, label) do
    value
    |> to_string()
    |> String.trim()
    |> case do
      "" -> {:error, "#{label} não pode ser vazio"}
      normalized -> {:ok, normalized}
    end
  end

  defp validate_docdb_uri(uri) do
    trimmed_uri = String.trim(uri)

    cond do
      String.starts_with?(trimmed_uri, "mongodb://") ->
        {:ok, trimmed_uri}

      String.starts_with?(trimmed_uri, "mongodb+srv://") ->
        {:error, "documentdb requer mongodb://. A URI recebida usa mongodb+srv://, que não é suportada pelo mongodump: #{inspect(trimmed_uri)}"}

      String.contains?(trimmed_uri, "://") ->
        {:error, "documentdb URI com formato inválido. Esperado mongodb://..., recebido: #{inspect(String.slice(trimmed_uri, 0, 80))}"}

      true ->
        {:error, "documentdb URI inválida: não contém esquema. Esperado mongodb://, recebido: #{inspect(String.slice(trimmed_uri, 0, 80))}"}
    end
  end

  defp normalize_prefix(value) do
    value
    |> to_string()
    |> String.trim()
    |> case do
      "" -> {:ok, @default_prefix}
      normalized ->
        sanitized =
          normalized
          |> String.trim_leading("/")
          |> String.replace(~r{/+}, "/")

        final_prefix =
          if String.ends_with?(sanitized, "/") do
            sanitized
          else
            sanitized <> "/"
          end

        {:ok, final_prefix}
    end
  end

  defp resolve_positive_integer(value, default_value, label) do
    candidate =
      case value do
        nil -> default_value
        explicit -> explicit
      end

    case candidate do
      integer when is_integer(integer) and integer > 0 ->
        {:ok, integer}

      _ ->
        {:error, "#{label} precisa ser inteiro positivo"}
    end
  end

  defp resolve_compression_level(nil), do: {:ok, 1}

  defp resolve_compression_level(level) when is_integer(level) and level >= 1 and level <= 9,
    do: {:ok, level}

  defp resolve_compression_level(_),
    do: {:error, "compression_level precisa estar entre 1 e 9"}

  defp resolve_expected_size_bytes(options) do
    expected_size_bytes = options[:expected_size_bytes]
    expected_size_gib = options[:expected_size_gib]

    cond do
      not is_nil(expected_size_bytes) and not is_nil(expected_size_gib) ->
        {:error, "use apenas expected_size_bytes ou expected_size_gib"}

      not is_nil(expected_size_bytes) ->
        resolve_positive_integer(expected_size_bytes, @default_expected_size_bytes, "expected_size_bytes")

      not is_nil(expected_size_gib) ->
        with {:ok, expected_size_gib_normalized} <-
               resolve_positive_integer(expected_size_gib, 10, "expected_size_gib") do
          {:ok, expected_size_gib_normalized * 1024 * 1024 * 1024}
        end

      true ->
        {:ok, @default_expected_size_bytes}
    end
  end

  defp resolve_mongodump_args(options) do
    extra_args =
      options
      |> Keyword.get_values(:mongodump_arg)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))

    with {:ok, _} <- validate_mongodump_connection_args(extra_args) do
      translated_args = parse_mongodump_option_compatibility(extra_args)
      {:ok, translated_args}
    end
  end

  defp validate_mongodump_connection_args(extra_args) do
    case Enum.find(extra_args, &contains_connection_string?/1) do
      nil -> {:ok, :ok}
      invalid_arg ->
        {:error,
         "não use --uri ou string de conexão em --mongodump-arg: #{inspect(invalid_arg)}\nA URI já é passada como primeiro argumento do script e enviada via --uri"}
    end
  end

  defp contains_connection_string?(arg) do
    normalized = String.trim(arg)

    is_uri_flag?(normalized) || contains_mongodb_scheme?(normalized)
  end

  defp is_uri_flag?(normalized) do
    normalized == "--uri" || String.starts_with?(normalized, "--uri=")
  end

  defp contains_mongodb_scheme?(normalized) do
    String.starts_with?(normalized, "mongodb://") || String.starts_with?(normalized, "mongodb+srv://")
  end

  defp default_num_parallel_collections do
    System.schedulers_online()
    |> max(16)
    |> min(32)
  end

  defp default_pigz_threads do
    System.schedulers_online()
    |> max(8)
    |> min(16)
  end

  defp ensure_binary(binary) do
    case System.find_executable(binary) do
      nil -> {:error, "binário obrigatório não encontrado no PATH: #{binary}"}
      _ -> :ok
    end
  end

  defp build_s3_key(prefix) do
    timestamp =
      DateTime.utc_now()
      |> DateTime.to_iso8601()
      |> String.replace([":", "-"], "")
      |> String.replace(".", "")

    {:ok, "#{prefix}docdb-backup-#{timestamp}.archive.gz"}
  end

  defp run_pipeline(args, key) do
    start = System.monotonic_time(:microsecond)
    destination = "s3://#{args.bucket}/#{key}"
    spinner = start_status_spinner("backup em andamento")

    mongodump_command =
      [
        "mongodump",
        "--uri",
        shell_escape(args.uri),
        "--archive",
        "--numParallelCollections",
        Integer.to_string(args.num_parallel_collections)
      ]
      |> Kernel.++(Enum.map(args.extra_mongodump_args, &shell_escape/1))
      |> Enum.join(" ")

    pigz_command =
      [
        "pigz",
        "-c",
        "-#{args.compression_level}",
        "-p",
        Integer.to_string(args.pigz_threads)
      ]
      |> Enum.join(" ")

    aws_command =
      [
        "aws",
        "s3",
        "cp",
        "-",
        shell_escape(destination),
        "--no-progress",
        "--only-show-errors",
        "--expected-size",
        Integer.to_string(args.expected_size_bytes)
      ]
      |> Enum.join(" ")

    pipeline =
      [mongodump_command, pigz_command, aws_command]
      |> Enum.join(" | ")

    status_probe = "__PIPESTATUS__"
    pipeline_status_command = """
set -o pipefail
#{pipeline}
pipeline_status=\"${PIPESTATUS[*]}\"
pipeline_exit=0
for status_code in ${pipeline_status}; do
  if [ \"$status_code\" != \"0\" ]; then
    pipeline_exit=1
    break
  fi
done
printf \"#{status_probe}=%s\\n\" \"${pipeline_status}\"
exit \"$pipeline_exit\"
"""

    print_config(args)
    IO.puts("destino: #{destination}")
    IO.puts("alvo: #{format_bytes_binary(@default_expected_size_bytes)} em até #{@default_target_duration_seconds}s")

    elapsed_us = fn -> System.monotonic_time(:microsecond) - start end

    case System.cmd("bash", ["-o", "pipefail", "-c", pipeline_status_command], stderr_to_stdout: true) do
      {output, 0} ->
        stop_status_spinner(spinner)
        print_pipeline_output(output, status_probe)
        {:ok,
         %{
           duration_us: elapsed_us.(),
           estimated_bytes: args.expected_size_bytes
         }}

      {output, status} ->
        stop_status_spinner(spinner)
        pipeline_status = extract_pipeline_status(output, status_probe)
        cleaned_output = remove_pipeline_status_line(output, status_probe)
        metrics = %{
          duration_us: elapsed_us.(),
          estimated_bytes: args.expected_size_bytes
        }

        if String.trim(cleaned_output) == "" do
          if pipeline_status == "" do
            {:error, "pipeline falhou com código #{status}", metrics}
          else
            {:error, "pipeline falhou com código #{status} | estágios (mongodump,pigz,aws): #{pipeline_status}", metrics}
          end
        else
          if pipeline_status == "" do
            {:error, "pipeline falhou com código #{status}\n#{cleaned_output}", metrics}
          else
            {:error, "pipeline falhou com código #{status} | estágios (mongodump,pigz,aws): #{pipeline_status}\n#{cleaned_output}", metrics}
          end
        end
    end
  end

  defp extract_pipeline_status(output, marker) do
    output
    |> String.split("\n", trim: true)
    |> Enum.find_value(fn line ->
      case String.split(line, "=", parts: 2) do
        [^marker, value] -> String.trim(value)
        _ -> nil
      end
    end) |> case do
      nil -> ""
      status -> status
    end
  end

  defp print_pipeline_output(output, marker) do
    output
    |> String.split("\n", trim: true)
    |> Enum.each(fn line ->
      case String.split(line, "=", parts: 2) do
        [^marker, _] ->
          :ok
        _ ->
          if String.trim(line) != "" do
            IO.puts(line)
          end
      end
    end)
  end

  defp remove_pipeline_status_line(output, marker) do
    output
    |> String.split("\n", trim: false)
    |> Enum.reject(fn line ->
      case String.split(String.trim(line), "=", parts: 2) do
        [^marker, _] -> true
        _ -> false
      end
    end)
    |> Enum.join("\n")
  end

  defp print_config(args) do
    IO.puts(
      "config: numParallelCollections=#{args.num_parallel_collections} pigz_threads=#{args.pigz_threads} compression_level=#{args.compression_level} expected_size=#{format_bytes_binary(args.expected_size_bytes)}"
    )
  end

  defp print_performance_report(metrics, expected_size_bytes) do
    expected_size_bytes =
      case expected_size_bytes do
        nil -> 0
        value when is_integer(value) and value > 0 -> value
        _ -> 0
      end

    duration_us = Map.get(metrics, :duration_us, 0)
    duration_seconds = max(1, div(duration_us, 1_000_000))

    IO.puts("tempo total: #{format_duration(metrics)}")

    estimated_bytes = Map.get(metrics, :estimated_bytes, expected_size_bytes)
    if estimated_bytes > 0 do
      throughput_mb_per_sec =
        estimated_bytes / 1024.0 / 1024.0 / duration_seconds

      IO.puts(
        "volume estimado: #{format_bytes_binary(estimated_bytes)} (~#{:erlang.float_to_binary(throughput_mb_per_sec, decimals: 2)} MiB/s)"
      )
    else
      IO.puts("volume estimado: não disponível")
    end

    target_duration_seconds = @default_target_duration_seconds
    target_speed_mib_per_sec = expected_size_bytes / 1024.0 / 1024.0 / target_duration_seconds
    target_status =
      if duration_us <= @default_target_duration_seconds * 1_000_000 do
        "atingido"
      else
        "não atingido"
      end

    target_gib_per_min = expected_size_bytes / 1024.0 / 1024.0 / 1024.0 / (target_duration_seconds / 60.0)
    IO.puts(
      "meta de throughput: #{:erlang.float_to_binary(target_speed_mib_per_sec, decimals: 2)} MiB/s (#{:erlang.float_to_binary(target_gib_per_min, decimals: 2)} GiB/min) | resultado: #{target_status}"
    )
  end

  defp format_bytes_binary(bytes) when is_integer(bytes) and bytes >= 0 do
    format_bytes_binary(bytes, 0, ["B", "KiB", "MiB", "GiB", "TiB"])
  end

  defp format_bytes_binary(bytes, _power, [_last]) when bytes >= 0 do
    formatted = bytes / :math.pow(1024, 4)
    "#{:erlang.float_to_binary(formatted, decimals: 2)} TiB"
  end

  defp format_bytes_binary(bytes, power, [unit | units]) do
    denominator = :math.pow(1024, power)
    if bytes < denominator * 1024 do
      "#{:erlang.float_to_binary(bytes / denominator, decimals: 2)} #{unit}"
    else
      format_bytes_binary(bytes, power + 1, units)
    end
  end

  defp format_duration(%{duration_us: duration_us}) do
    total_seconds = div(duration_us, 1_000_000)
    minutes = div(total_seconds, 60)
    seconds = rem(total_seconds, 60)
    if minutes > 0 do
      "#{minutes}m#{String.pad_leading(Integer.to_string(seconds), 2, "0")}s"
    else
      "#{seconds}s"
    end
  end

  defp format_duration(_), do: "0s"

  defp start_status_spinner(message) do
    spawn(fn -> status_spinner_loop(message, 0) end)
  end

  defp status_spinner_loop(message, frame_idx) do
    frames = ["|", "/", "-", "\\"]

    receive do
      :stop ->
        IO.write("\r")
        IO.write(String.duplicate(" ", 80))
        IO.write("\r")
        :ok
    after
      250 ->
        frame = Enum.at(frames, rem(frame_idx, length(frames)))
        IO.write("\r#{message} #{frame}")
        status_spinner_loop(message, frame_idx + 1)
    end
  end

  defp stop_status_spinner(pid) when is_pid(pid) do
    send(pid, :stop)
    :ok
  end

  defp shell_escape(value) do
    escaped = String.replace(value, "'", "'\\''")
    "'#{escaped}'"
  end
end

DocdbStreamBackup.main(System.argv())
