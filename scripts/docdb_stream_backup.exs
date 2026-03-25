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
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> --mongodump-arg --tls --mongodump-arg --tlsCAFile=/path/ca.pem
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> --mongodump-arg='--tls' --mongodump-arg='--tlsCAFile=/path/ca.pem'

  Exemplos:
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false' meu-bucket
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket docdb/prod
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket --num-parallel-collections 16 --pigz-threads 8 --compression-level 1 --expected-size-bytes 10737418240

  Observação:
    O upload acontece por stream em memória, sem gerar arquivo local no EC2.
    Perfil padrão otimizado para throughput: compressão nível 1 e expected-size de 10 GiB.
    Meta de desempenho: 10 GiB em até 60 segundos.
    A string de conexão principal é o primeiro argumento posicional.
    Não passe --uri novamente em --mongodump-arg.
  """

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

  @legacy_tls_query_to_ssl %{
    "tls" => "ssl",
    "tlsAllowInvalidCertificates" => "sslAllowInvalidCertificates",
    "tlsAllowInvalidHostnames" => "sslAllowInvalidHostnames",
    "tlsCAFile" => "sslCAFile",
    "tlsCRLFile" => "sslCRLFile",
    "tlsCertificateKeyFile" => "sslPEMKeyFile",
    "tlsCertificateKeyFilePassword" => "sslPEMKeyPassword",
    "tlsDisabledProtocols" => "sslDisabledProtocols",
    "tlsInsecure" => "sslInsecure",
    "tlsFIPSMode" => "sslFIPSMode"
  }

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
             capabilities <- inspect_mongodump_capabilities(),
             {:ok, compatible_args} <- apply_mongodump_compatibility(args, capabilities),
             {:ok, key} <- build_s3_key(compatible_args.prefix),
             {:ok, metrics} <- run_pipeline(compatible_args, capabilities, key) do
          print_performance_report(metrics, compatible_args.expected_size_bytes)
          IO.puts("backup concluído")
          IO.puts("destino: s3://#{compatible_args.bucket}/#{key}")
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
        {:error, "opções inválidas: #{Enum.map_join(invalid_options, ", ", &format_invalid_option/1)}"}

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

  defp normalize_mongodump_arg_syntax(argv), do: normalize_mongodump_arg_syntax(argv, [])

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

  defp parse_positional_args([uri, bucket]), do: {:ok, %{uri: uri, bucket: bucket, prefix: nil}}
  defp parse_positional_args([uri, bucket, prefix]), do: {:ok, %{uri: uri, bucket: bucket, prefix: prefix}}
  defp parse_positional_args(_), do: {:error, "argumentos inválidos"}

  defp format_invalid_option({option, nil}), do: to_string(option)
  defp format_invalid_option({option, value}), do: "#{option}=#{inspect(value)}"

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
        {:error, "documentdb requer mongodb://, mas a URI recebida usa mongodb+srv://: #{trimmed_uri}"}

      String.contains?(trimmed_uri, "://") ->
        {:error, "documentdb URI com formato inválido; esperado mongodb://..., recebido: #{preview(trimmed_uri)}"}

      true ->
        {:error, "documentdb URI inválida (esperado mongodb://): #{preview(trimmed_uri)}"}
    end
  end

  defp preview(value) do
    if String.length(value) <= 80 do
      value
    else
      String.slice(value, 0, 80) <> "..."
    end
  end

  defp resolve_prefix(nil, nil), do: {:ok, @default_prefix}
  defp resolve_prefix(nil, prefix), do: normalize_prefix(prefix)
  defp resolve_prefix(prefix, nil), do: normalize_prefix(prefix)
  defp resolve_prefix(_positional_prefix, _option_prefix), do: {:error, "use prefix posicional ou --prefix, não os dois"}

  defp normalize_prefix(value) do
    value
    |> to_string()
    |> String.trim()
    |> case do
      "" ->
        {:ok, @default_prefix}

      normalized ->
        normalized
        |> String.trim_leading("/")
        |> String.replace(~r{/+}, "/")
        |> case do
          "" -> {:ok, @default_prefix}
          sanitized ->
            if String.ends_with?(sanitized, "/") do
              {:ok, sanitized}
            else
              {:ok, sanitized <> "/"}
            end
        end
    end
  end

  defp resolve_positive_integer(nil, default_value, label), do: resolve_positive_integer(default_value, default_value, label)

  defp resolve_positive_integer(value, _default_value, _label) when is_integer(value) and value > 0,
    do: {:ok, value}

  defp resolve_positive_integer(_value, _default_value, label),
    do: {:error, "#{label} precisa ser inteiro positivo"}

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
        with {:ok, parsed_gib} <- resolve_positive_integer(expected_size_gib, 10, "expected_size_gib") do
          {:ok, parsed_gib * 1024 * 1024 * 1024}
        end

      true ->
        {:ok, @default_expected_size_bytes}
    end
  end

  defp resolve_mongodump_args(options) do
    options
    |> Keyword.get_values(:mongodump_arg)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> validate_mongodump_connection_args()
  end

  defp validate_mongodump_connection_args(args) do
    case Enum.find(args, &uri_connection_arg?/1) do
      nil ->
        {:ok, args}

      invalid_arg ->
        {:error,
         "não use --uri ou string de conexão em --mongodump-arg: #{inspect(invalid_arg)}\nA URI já é passada como primeiro argumento do script e enviada via --uri"}
    end
  end

  defp uri_connection_arg?(value) do
    normalized = String.trim(value)

    normalized == "--uri" or
      String.starts_with?(normalized, "--uri=") or
      String.starts_with?(normalized, "mongodb://") or
      String.starts_with?(normalized, "mongodb+srv://")
  end

  defp ensure_binary(binary) do
    case System.find_executable(binary) do
      nil -> {:error, "binário obrigatório não encontrado no PATH: #{binary}"}
      _ -> :ok
    end
  end

  defp inspect_mongodump_capabilities do
    case System.cmd("mongodump", ["--help"], stderr_to_stdout: true) do
      {help_text, 0} ->
        %{
          help_available: true,
          supports_quiet: flag_supported?(help_text, "--quiet"),
          supports_tls: flag_supported?(help_text, "--tls"),
          supports_ssl: flag_supported?(help_text, "--ssl"),
          supports_num_parallel_collections: flag_supported?(help_text, "--numParallelCollections")
        }

      _ ->
        %{
          help_available: false,
          supports_quiet: false,
          supports_tls: false,
          supports_ssl: false,
          supports_num_parallel_collections: false
        }
    end
  rescue
    _ ->
      %{
        help_available: false,
        supports_quiet: false,
        supports_tls: false,
        supports_ssl: false,
        supports_num_parallel_collections: false
      }
  end

  defp flag_supported?(text, flag) do
    regex = ~r/(^|\s)#{Regex.escape(flag)}(\s|=|,)/
    String.contains?(text, flag) and Regex.match?(regex, text)
  end

  defp apply_mongodump_compatibility(args, capabilities) do
    {:ok,
     %{
       args
       | uri: normalize_tls_uri_query(args.uri, capabilities),
         extra_mongodump_args: normalize_mongodump_args(args.extra_mongodump_args, capabilities)
     }}
  end

  defp normalize_tls_uri_query(uri, %{help_available: false}), do: uri
  defp normalize_tls_uri_query(uri, %{supports_tls: true}), do: uri
  defp normalize_tls_uri_query(uri, %{supports_ssl: false}), do: uri

  defp normalize_tls_uri_query(uri, _capabilities) do
    parsed_uri = URI.parse(uri)

    if is_nil(parsed_uri.query) or parsed_uri.query == "" do
      uri
    else
      original_query = URI.decode_query(parsed_uri.query)

      normalized_query =
        Enum.reduce(@legacy_tls_query_to_ssl, original_query, fn {legacy_key, replacement_key}, query ->
          case Map.pop(query, legacy_key) do
            {nil, remaining_query} ->
              remaining_query

            {value, remaining_query} ->
              if Map.has_key?(remaining_query, replacement_key) do
                remaining_query
              else
                Map.put(remaining_query, replacement_key, value)
              end
          end
        end)

      if normalized_query == original_query do
        uri
      else
        %{parsed_uri | query: URI.encode_query(normalized_query)}
        |> URI.to_string()
      end
    end
  end

  defp normalize_mongodump_args(args, capabilities) do
    args
    |> Enum.map(&translate_tls_arg(&1, capabilities))
  end

  defp translate_tls_arg(arg, %{help_available: false}), do: arg
  defp translate_tls_arg(arg, %{supports_tls: true}), do: arg
  defp translate_tls_arg(arg, %{supports_ssl: false}), do: arg

  defp translate_tls_arg(arg, _capabilities) do
    case String.split(arg, "=", parts: 2) do
      [flag, value] ->
        case Map.fetch(@legacy_tls_to_ssl, flag) do
          {:ok, replacement} -> replacement <> "=" <> value
          :error -> arg
        end

      [flag] ->
        Map.get(@legacy_tls_to_ssl, flag, arg)
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

  defp run_pipeline(args, capabilities, key) do
    started_at = System.monotonic_time(:microsecond)
    destination = "s3://#{args.bucket}/#{key}"
    progress_display = start_progress_display("backup em andamento", ["mongodump", "pigz", "aws"])

    mongodump_args =
      ["mongodump", "--uri", args.uri, "--archive"]
      |> Kernel.++(num_parallel_collections_flag(args.num_parallel_collections, capabilities))
      |> Kernel.++(args.extra_mongodump_args)

    pigz_args = ["pigz", "-c", "-#{args.compression_level}", "-p", Integer.to_string(args.pigz_threads)]

    aws_args = [
      "aws",
      "s3",
      "cp",
      "-",
      destination,
      "--no-progress",
      "--only-show-errors",
      "--expected-size",
      Integer.to_string(args.expected_size_bytes)
    ]

    pipeline_summary = [
      {"mongodump", format_logged_command(mongodump_args)},
      {"pigz", Enum.join(pigz_args, " ")},
      {"aws", Enum.join(aws_args, " ")}
    ]

    status_probe = "__PIPESTATUS__"
    stderr_markers = [
      {"mongodump", "__STDERR_MONGODUMP_BEGIN__", "__STDERR_MONGODUMP_END__"},
      {"pigz", "__STDERR_PIGZ_BEGIN__", "__STDERR_PIGZ_END__"},
      {"aws", "__STDERR_AWS_BEGIN__", "__STDERR_AWS_END__"}
    ]

    mongodump_command = Enum.map_join(mongodump_args, " ", &shell_escape/1)
    pigz_command = Enum.map_join(pigz_args, " ", &shell_escape/1)
    aws_command = Enum.map_join(aws_args, " ", &shell_escape/1)

    command = """
    set -o pipefail
    stderr_mongodump="$(mktemp)"
    stderr_pigz="$(mktemp)"
    stderr_aws="$(mktemp)"
    cleanup() {
      rm -f "${stderr_mongodump}" "${stderr_pigz}" "${stderr_aws}"
    }
    trap cleanup EXIT
    #{mongodump_command} 2>"${stderr_mongodump}" | #{pigz_command} 2>"${stderr_pigz}" | #{aws_command} 2>"${stderr_aws}"
    pipeline_status="${PIPESTATUS[*]}"
    pipeline_exit=0
    for status_code in ${pipeline_status}; do
      if [ "$status_code" != "0" ]; then
        pipeline_exit=1
        break
      fi
    done
    printf "#{status_probe}=%s\\n" "${pipeline_status}"
    printf "__STDERR_MONGODUMP_BEGIN__\\n"
    cat "${stderr_mongodump}"
    printf "\\n__STDERR_MONGODUMP_END__\\n"
    printf "__STDERR_PIGZ_BEGIN__\\n"
    cat "${stderr_pigz}"
    printf "\\n__STDERR_PIGZ_END__\\n"
    printf "__STDERR_AWS_BEGIN__\\n"
    cat "${stderr_aws}"
    printf "\\n__STDERR_AWS_END__\\n"
    exit "${pipeline_exit}"
    """

    print_config(args, capabilities)
    IO.puts("destino: #{destination}")
    IO.puts("alvo: #{format_bytes_binary(@default_expected_size_bytes)} em até #{@default_target_duration_seconds}s")

    case System.cmd("bash", ["-c", command], stderr_to_stdout: true) do
      {output, 0} ->
        stop_progress_display(progress_display, success_stage_states())
        print_pipeline_output(output, status_probe, stderr_markers)

        {:ok,
         %{
           duration_us: System.monotonic_time(:microsecond) - started_at,
           raw_bytes: 0,
           estimated_bytes: args.expected_size_bytes
         }}

      {output, status} ->
        pipeline_status = extract_pipeline_status(output, status_probe)
        stop_progress_display(progress_display, final_stage_states(pipeline_status))
        cleaned_output = remove_probe_sections(output, status_probe, stderr_markers)
        failed_stages = failed_pipeline_stages(pipeline_status)
        stderr_sections = extract_stderr_sections(output, stderr_markers)

        details =
          [
            "pipeline falhou com código #{status}",
            format_pipeline_stage_details(failed_stages),
            format_failed_commands(pipeline_summary, failed_stages),
            format_stage_stderr(stderr_sections, failed_stages),
            format_pipeline_output(cleaned_output)
          ]
          |> Enum.reject(&(&1 == ""))
          |> Enum.join("\n")

        {:error,
         details,
         %{
           duration_us: System.monotonic_time(:microsecond) - started_at,
           raw_bytes: 0,
           estimated_bytes: 0
         }}
    end
  end

  defp num_parallel_collections_flag(_num_parallel_collections, %{supports_num_parallel_collections: false}), do: []

  defp num_parallel_collections_flag(num_parallel_collections, _capabilities) do
    ["--numParallelCollections", Integer.to_string(num_parallel_collections)]
  end

  defp format_pipeline_stage_details([]), do: ""

  defp format_pipeline_stage_details(failed_stages) do
    "falha identificada: " <> Enum.join(failed_stages, ", ")
  end

  defp format_failed_commands(pipeline_summary, failed_stages) do
    failed_stage_names =
      failed_stages
      |> Enum.map(fn stage_status ->
        stage_status
        |> String.split("=", parts: 2)
        |> hd()
      end)
      |> MapSet.new()

    failed_commands =
      pipeline_summary
      |> Enum.filter(fn {stage, _command} -> MapSet.member?(failed_stage_names, stage) end)
      |> Enum.map(fn {stage, command} -> "#{stage}: #{command}" end)

    case failed_commands do
      [] -> ""
      _ -> "comando(s) falho(s):\n" <> Enum.join(failed_commands, "\n")
    end
  end

  defp format_stage_stderr(stderr_sections, failed_stages) do
    failed_stage_names =
      failed_stages
      |> Enum.map(fn stage_status ->
        stage_status
        |> String.split("=", parts: 2)
        |> hd()
      end)

    formatted_sections =
      failed_stage_names
      |> Enum.map(fn stage_name ->
        case Map.get(stderr_sections, stage_name, "") |> String.trim() do
          "" -> nil
          stderr_output -> "#{stage_name}:\n#{stderr_output}"
        end
      end)
      |> Enum.reject(&is_nil/1)

    case formatted_sections do
      [] -> ""
      _ -> "stderr detalhado:\n" <> Enum.join(formatted_sections, "\n\n")
    end
  end

  defp format_pipeline_output(output) do
    trimmed = String.trim(output)

    if trimmed == "" do
      ""
    else
      "saida:\n" <> trimmed
    end
  end

  defp extract_pipeline_status(output, marker) do
    output
    |> String.split("\n", trim: true)
    |> Enum.find_value("", fn line ->
      case String.split(line, "=", parts: 2) do
        [^marker, value] -> String.trim(value)
        _ -> nil
      end
    end)
  end

  defp failed_pipeline_stages(status_line) do
    status_line
    |> String.split(" ", trim: true)
    |> Enum.with_index()
    |> Enum.map(fn {status, index} ->
      stage = Enum.at(["mongodump", "pigz", "aws"], index, "etapa-#{index + 1}")
      {stage, status}
    end)
    |> Enum.filter(fn {_stage, status} -> status != "0" and status != "" end)
    |> Enum.map(fn {stage, status} -> "#{stage}=#{status}" end)
  end

  defp extract_stderr_sections(output, stderr_markers) do
    stderr_markers
    |> Enum.map(fn {stage_name, begin_marker, end_marker} ->
      {stage_name, extract_marked_block(output, begin_marker, end_marker)}
    end)
    |> Map.new()
  end

  defp remove_probe_sections(output, marker, stderr_markers) do
    cleaned_output = remove_pipeline_status_line(output, marker)

    stderr_markers
    |> Enum.reduce(cleaned_output, fn {_stage, begin_marker, end_marker}, acc ->
      remove_marked_block(acc, begin_marker, end_marker)
    end)
    |> String.trim()
  end

  defp remove_pipeline_status_line(output, marker) do
    output
    |> String.split("\n", trim: false)
    |> Enum.reject(fn line ->
      trimmed_line = String.trim(line)

      case String.split(line, "=", parts: 2) do
        [^marker, _] -> true
        _ -> false
      end or trimmed_line == marker
    end)
    |> Enum.join("\n")
  end

  defp remove_marked_block(output, begin_marker, end_marker) do
    regex = ~r/#{Regex.escape(begin_marker)}\n?(.*?)\n?#{Regex.escape(end_marker)}/s
    Regex.replace(regex, output, "")
  end

  defp extract_marked_block(output, begin_marker, end_marker) do
    regex = ~r/#{Regex.escape(begin_marker)}\n?(.*?)\n?#{Regex.escape(end_marker)}/s

    case Regex.run(regex, output, capture: :all_but_first) do
      [content] -> String.trim(content)
      _ -> ""
    end
  end

  defp print_pipeline_output(output, marker, stderr_markers) do
    output
    |> remove_probe_sections(marker, stderr_markers)
    |> String.split("\n", trim: true)
    |> Enum.reject(&(String.trim(&1) == ""))
    |> Enum.each(&IO.puts/1)
  end

  defp format_logged_command(["mongodump" | args]) do
    ["mongodump" | sanitize_connection_args(args)]
    |> Enum.join(" ")
  end

  defp format_logged_command(args), do: Enum.join(args, " ")

  defp sanitize_connection_args(args), do: sanitize_connection_args(args, [])

  defp sanitize_connection_args([], acc), do: Enum.reverse(acc)

  defp sanitize_connection_args(["--uri", uri | tail], acc) do
    sanitize_connection_args(tail, [mask_connection_uri(uri), "--uri" | acc])
  end

  defp sanitize_connection_args([arg | tail], acc) do
    if String.starts_with?(arg, "--uri=") do
      uri = String.trim_leading(arg, "--uri=")
      sanitize_connection_args(tail, ["--uri=#{mask_connection_uri(uri)}" | acc])
    else
      sanitize_connection_args(tail, [arg | acc])
    end
  end

  defp mask_connection_uri(uri) do
    parsed = URI.parse(uri)

    case parsed.userinfo do
      nil ->
        uri

      userinfo ->
        masked_userinfo =
          case String.split(userinfo, ":", parts: 2) do
            [user, _password] -> user <> ":***"
            [user] -> user
            _ -> "***"
          end

        String.replace(uri, "#{userinfo}@", "#{masked_userinfo}@", global: false)
    end
  end

  defp print_config(args, capabilities) do
    num_parallel_display =
      if capabilities.supports_num_parallel_collections do
        Integer.to_string(args.num_parallel_collections)
      else
        "desativado"
      end

    IO.puts(
      "config: numParallelCollections=#{num_parallel_display} pigz_threads=#{args.pigz_threads} compression_level=#{args.compression_level} expected_size=#{format_bytes_binary(args.expected_size_bytes)}"
    )
  end

  defp print_performance_report(metrics, expected_size_bytes) do
    duration_us = Map.get(metrics, :duration_us, 0)
    duration_seconds = max(1, div(duration_us, 1_000_000))
    raw_bytes = Map.get(metrics, :raw_bytes, 0)
    estimated_bytes = Map.get(metrics, :estimated_bytes, 0)

    IO.puts("tempo total: #{format_duration(duration_us)}")

    if raw_bytes > 0 do
      throughput = raw_bytes / 1024.0 / 1024.0 / duration_seconds
      IO.puts("volume processado: #{format_bytes_binary(raw_bytes)} (~#{:erlang.float_to_binary(throughput, decimals: 2)} MiB/s)")
    else
      if estimated_bytes > 0 do
        throughput = estimated_bytes / 1024.0 / 1024.0 / duration_seconds
        IO.puts("volume estimado: #{format_bytes_binary(estimated_bytes)} (~#{:erlang.float_to_binary(throughput, decimals: 2)} MiB/s)")
      else
        IO.puts("volume processado: sem bytes (não foi possível mensurar)")
      end
    end

    target_duration_seconds = @default_target_duration_seconds
    target_speed_mib_per_sec = expected_size_bytes / 1024.0 / 1024.0 / target_duration_seconds
    target_gib_per_min = expected_size_bytes / 1024.0 / 1024.0 / 1024.0 / (target_duration_seconds / 60.0)

    result =
      if duration_us <= @default_target_duration_seconds * 1_000_000 do
        "atingido"
      else
        "não atingido"
      end

    IO.puts(
      "meta de throughput: #{:erlang.float_to_binary(target_speed_mib_per_sec, decimals: 2)} MiB/s (#{:erlang.float_to_binary(target_gib_per_min, decimals: 2)} GiB/min) | resultado: #{result}"
    )
  end

  defp format_bytes_binary(bytes) when is_integer(bytes) and bytes <= 0, do: "0 B"

  defp format_bytes_binary(bytes) do
    do_format_bytes_binary(bytes / 1.0, ["B", "KiB", "MiB", "GiB", "TiB"])
  end

  defp do_format_bytes_binary(value, [unit | _rest]) when value < 1024,
    do: "#{:erlang.float_to_binary(value, decimals: 2)} #{unit}"

  defp do_format_bytes_binary(value, [_unit | rest]), do: do_format_bytes_binary(value / 1024.0, rest)

  defp format_duration(duration_us) do
    total_seconds = div(duration_us, 1_000_000)
    minutes = div(total_seconds, 60)
    seconds = rem(total_seconds, 60)

    if minutes > 0 do
      "#{minutes}m#{String.pad_leading(Integer.to_string(seconds), 2, "0")}s"
    else
      "#{seconds}s"
    end
  end

  defp success_stage_states do
    %{
      "mongodump" => {:done, 0},
      "pigz" => {:done, 0},
      "aws" => {:done, 0}
    }
  end

  defp final_stage_states(status_line) do
    status_line
    |> String.split(" ", trim: true)
    |> Enum.with_index()
    |> Enum.map(fn {status, index} ->
      stage = Enum.at(["mongodump", "pigz", "aws"], index, "etapa-#{index + 1}")
      parsed_status = parse_stage_status(status)
      stage_state =
        if parsed_status == 0 do
          {:done, parsed_status}
        else
          {:failed, parsed_status}
        end

      {stage, stage_state}
    end)
    |> Enum.into(%{})
  end

  defp parse_stage_status(status) do
    case Integer.parse(status) do
      {parsed_status, ""} -> parsed_status
      _ -> 1
    end
  end

  defp start_progress_display(message, stage_names) do
    ansi_enabled = IO.ANSI.enabled?()

    initial_state = %{
      message: message,
      stage_names: stage_names,
      stage_states: Map.new(stage_names, &{&1, {:running, nil}}),
      started_at: System.monotonic_time(:millisecond),
      frame: 0,
      ansi_enabled: ansi_enabled,
      first_render?: true
    }

    spawn(fn -> progress_display_loop(render_progress_display(initial_state)) end)
  end

  defp progress_display_loop(state) do
    receive do
      {:stop, stage_states} ->
        state
        |> Map.put(:stage_states, stage_states)
        |> render_progress_display()
        |> finalize_progress_display()
    after
      1_000 ->
        state
        |> Map.update!(:frame, &(&1 + 1))
        |> render_progress_display()
        |> progress_display_loop()
    end
  end

  defp finalize_progress_display(state) do
    if state.ansi_enabled do
      IO.write(IO.ANSI.reset())
    end

    :ok
  end

  defp stop_progress_display(pid, stage_states) do
    send(pid, {:stop, stage_states})
    :ok
  end

  defp render_progress_display(state) do
    lines = build_progress_lines(state)

    if state.ansi_enabled do
      if state.first_render? do
        IO.write(Enum.join(lines, "\n") <> "\n")
      else
        IO.write(IO.ANSI.cursor_up(length(lines)))
        Enum.each(lines, fn line ->
          IO.write(IO.ANSI.clear_line())
          IO.write(line <> "\n")
        end)
      end
    else
      should_print? = state.first_render? or rem(state.frame, 5) == 0

      if should_print? do
        IO.puts(Enum.join(lines, " | "))
      end
    end

    %{state | first_render?: false}
  end

  defp build_progress_lines(state) do
    elapsed_seconds = div(System.monotonic_time(:millisecond) - state.started_at, 1000)

    [
      "#{state.message} (#{elapsed_seconds}s)"
      | Enum.map(state.stage_names, fn stage_name ->
          format_progress_stage_line(stage_name, Map.get(state.stage_states, stage_name, {:running, nil}), state.frame)
        end)
    ]
  end

  defp format_progress_stage_line(stage_name, {:running, _status}, frame) do
    "#{String.pad_trailing(stage_name, 10)} #{indeterminate_bar(frame, 20)} running"
  end

  defp format_progress_stage_line(stage_name, {:done, status}, _frame) do
    "#{String.pad_trailing(stage_name, 10)} #{String.duplicate("#", 20)} done (#{status})"
  end

  defp format_progress_stage_line(stage_name, {:failed, status}, _frame) do
    "#{String.pad_trailing(stage_name, 10)} #{String.duplicate("!", 20)} failed (#{status})"
  end

  defp indeterminate_bar(frame, width) do
    active_size = 5
    travel = max(width - active_size, 1)
    start_index = rem(frame, travel + 1)

    0..(width - 1)
    |> Enum.map(fn index ->
      if index >= start_index and index < start_index + active_size do
        "#"
      else
        "."
      end
    end)
    |> Enum.join()
  end

  defp shell_escape(value) do
    escaped = String.replace(value, "'", "'\\''")
    "'#{escaped}'"
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
end

DocdbStreamBackup.main(System.argv())
