#!/usr/bin/env elixir

defmodule DocdbStreamBackup do
  @default_prefix "docdb/"
  @default_expected_size_bytes 10 * 1024 * 1024 * 1024
  @default_target_duration_seconds 60

  @usage """
  Uso:
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket>
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> <prefix>
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> --prefix docdb/prod
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> --mongodump-arg --tls --mongodump-arg --tlsCAFile=/path/ca.pem
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> --mongodump-arg='--tls' --mongodump-arg='--tlsCAFile=/path/ca.pem'

  Exemplos:
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false' meu-bucket
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket docdb/prod
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket --prefix docdb/prod --mongodump-arg --tlsCAFile=/path/ca.pem

  Observação:
    O upload acontece por stream em memória, sem gerar arquivo local no EC2.
    O script decide automaticamente entre pipeline único e paralelismo por database conforme CPU, RAM, número de databases e distribuição de volume do cluster.
    Os defaults de paralelismo, compressão, tuning do multipart do S3 e medição do stream são ajustados automaticamente por RAM/CPU do host para reduzir risco de OOM e maximizar throughput.
    Quando fizer sentido, cada database gera um objeto separado sob o prefixo docdb-backup-<timestamp>/.
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

  @legacy_ssl_to_tls Map.new(@legacy_tls_to_ssl, fn {tls_flag, ssl_flag} ->
                       {ssl_flag, tls_flag}
                     end)
  @legacy_ssl_query_to_tls Map.new(@legacy_tls_query_to_ssl, fn {tls_key, ssl_key} ->
                             {ssl_key, tls_key}
                           end)

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
             {:ok, outcome} <- run_backup(compatible_args, capabilities) do
          print_performance_report(outcome.metrics, compatible_args.expected_size_bytes)
          print_backup_summary(outcome)
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
          parallel_databases: :boolean,
          database: :keep,
          database_concurrency: :integer,
          num_parallel_collections: :integer,
          pigz_threads: :integer,
          compression_level: :integer,
          s3_max_concurrent_requests: :integer,
          s3_max_queue_size: :integer,
          s3_multipart_chunksize_mib: :integer,
          meter_block_size_mib: :integer,
          expected_size_bytes: :integer,
          expected_size_gib: :integer,
          mongodump_arg: :keep
        ],
        aliases: [
          h: :help,
          p: :prefix
        ]
      )

    runtime_tuning = default_runtime_tuning()

    cond do
      options[:help] ->
        {:help, @usage}

      invalid_options != [] ->
        {:error,
         "opções inválidas: #{Enum.map_join(invalid_options, ", ", &format_invalid_option/1)}"}

      true ->
        with {:ok, positional} <- parse_positional_args(positional_args),
             {:ok, normalized_uri} <- normalize_non_empty(positional.uri, "docdb_uri"),
             {:ok, validated_uri} <- validate_docdb_uri(normalized_uri),
             {:ok, normalized_bucket} <- normalize_non_empty(positional.bucket, "bucket"),
             {:ok, normalized_prefix} <- resolve_prefix(positional.prefix, options[:prefix]),
             {:ok, database_names} <- resolve_database_names(options),
             {:ok, parallel_databases} <- resolve_parallel_databases(options, database_names),
             {:ok, database_concurrency} <-
               resolve_database_concurrency(
                 options[:database_concurrency],
                 runtime_tuning,
                 parallel_databases
               ),
             {:ok, num_parallel_collections} <-
               resolve_positive_integer(
                 options[:num_parallel_collections],
                 runtime_tuning.num_parallel_collections,
                 "num_parallel_collections"
               ),
             {:ok, pigz_threads} <-
               resolve_positive_integer(
                 options[:pigz_threads],
                 runtime_tuning.pigz_threads,
                 "pigz_threads"
               ),
             {:ok, compression_level} <-
               resolve_compression_level(options[:compression_level], runtime_tuning),
             {:ok, s3_max_concurrent_requests} <-
               resolve_positive_integer(
                 options[:s3_max_concurrent_requests],
                 runtime_tuning.s3_max_concurrent_requests,
                 "s3_max_concurrent_requests"
               ),
             {:ok, s3_max_queue_size} <-
               resolve_positive_integer(
                 options[:s3_max_queue_size],
                 runtime_tuning.s3_max_queue_size,
                 "s3_max_queue_size"
               ),
             {:ok, s3_multipart_chunksize_mib} <-
               resolve_positive_integer(
                 options[:s3_multipart_chunksize_mib],
                 runtime_tuning.s3_multipart_chunksize_mib,
                 "s3_multipart_chunksize_mib"
               ),
             {:ok, meter_block_size_mib} <-
               resolve_positive_integer(
                 options[:meter_block_size_mib],
                 runtime_tuning.meter_block_size_mib,
                 "meter_block_size_mib"
               ),
             {:ok, expected_size_bytes} <- resolve_expected_size_bytes(options),
             {:ok, extra_mongodump_args} <- resolve_mongodump_args(options),
             :ok <-
               validate_parallel_database_args(
                 parallel_databases,
                 database_names,
                 extra_mongodump_args
               ) do
          {:ok,
           %{
             uri: validated_uri,
             bucket: normalized_bucket,
             prefix: normalized_prefix,
             parallel_databases: parallel_databases,
             database_names: database_names,
             database_concurrency: database_concurrency,
             num_parallel_collections: num_parallel_collections,
             pigz_threads: pigz_threads,
             compression_level: compression_level,
             s3_max_concurrent_requests: s3_max_concurrent_requests,
             s3_max_queue_size: s3_max_queue_size,
             s3_multipart_chunksize_mib: s3_multipart_chunksize_mib,
             meter_block_size_mib: meter_block_size_mib,
             expected_size_bytes: expected_size_bytes,
             extra_mongodump_args: extra_mongodump_args,
             runtime_tuning: runtime_tuning,
             num_parallel_collections_source: option_source(options[:num_parallel_collections]),
             pigz_threads_source: option_source(options[:pigz_threads]),
             compression_level_source: option_source(options[:compression_level]),
             s3_max_concurrent_requests_source:
               option_source(options[:s3_max_concurrent_requests]),
             s3_max_queue_size_source: option_source(options[:s3_max_queue_size]),
             s3_multipart_chunksize_mib_source:
               option_source(options[:s3_multipart_chunksize_mib]),
             meter_block_size_mib_source: option_source(options[:meter_block_size_mib])
           }}
        end
    end
  end

  defp normalize_mongodump_arg_syntax(argv), do: normalize_mongodump_arg_syntax(argv, [])

  defp normalize_mongodump_arg_syntax([], acc), do: {:ok, Enum.reverse(acc)}

  defp normalize_mongodump_arg_syntax(["--mongodump-arg" | tail], acc) do
    case tail do
      [] ->
        {:error,
         "opção --mongodump-arg requer valor. Ex.: --mongodump-arg=--tls ou --mongodump-arg --tls"}

      [value | rest] ->
        normalize_mongodump_arg_syntax(rest, ["--mongodump-arg=#{value}" | acc])
    end
  end

  defp normalize_mongodump_arg_syntax([arg | tail], acc),
    do: normalize_mongodump_arg_syntax(tail, [arg | acc])

  defp parse_positional_args([uri, bucket]), do: {:ok, %{uri: uri, bucket: bucket, prefix: nil}}

  defp parse_positional_args([uri, bucket, prefix]),
    do: {:ok, %{uri: uri, bucket: bucket, prefix: prefix}}

  defp parse_positional_args(_), do: {:error, "argumentos inválidos"}

  defp resolve_database_names(options) do
    database_names =
      options
      |> Keyword.get_values(:database)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.uniq()

    {:ok, database_names}
  end

  defp resolve_parallel_databases(options, database_names) do
    {:ok,
     options[:parallel_databases] == true or database_names != [] or
       not is_nil(options[:database_concurrency])}
  end

  defp resolve_database_concurrency(nil, runtime_tuning, _parallel_databases) do
    {:ok, default_database_concurrency(runtime_tuning)}
  end

  defp resolve_database_concurrency(database_concurrency, _runtime_tuning, _parallel_databases) do
    resolve_positive_integer(database_concurrency, database_concurrency, "database_concurrency")
  end

  defp validate_parallel_database_args(false, _database_names, _extra_mongodump_args), do: :ok

  defp validate_parallel_database_args(true, _database_names, extra_mongodump_args) do
    case Enum.find(extra_mongodump_args, &parallel_database_incompatible_arg?/1) do
      nil ->
        :ok

      invalid_arg ->
        {:error,
         "no modo --parallel-databases não use --db/--collection em --mongodump-arg: #{inspect(invalid_arg)}"}
    end
  end

  defp parallel_database_incompatible_arg?(arg) do
    normalized_arg = String.trim(arg)

    String.starts_with?(normalized_arg, "--db") or
      String.starts_with?(normalized_arg, "--collection")
  end

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
        {:error,
         "documentdb requer mongodb://, mas a URI recebida usa mongodb+srv://: #{trimmed_uri}"}

      String.contains?(trimmed_uri, "://") ->
        {:error,
         "documentdb URI com formato inválido; esperado mongodb://..., recebido: #{preview(trimmed_uri)}"}

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

  defp resolve_prefix(_positional_prefix, _option_prefix),
    do: {:error, "use prefix posicional ou --prefix, não os dois"}

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
          "" ->
            {:ok, @default_prefix}

          sanitized ->
            if String.ends_with?(sanitized, "/") do
              {:ok, sanitized}
            else
              {:ok, sanitized <> "/"}
            end
        end
    end
  end

  defp resolve_positive_integer(nil, default_value, label),
    do: resolve_positive_integer(default_value, default_value, label)

  defp resolve_positive_integer(value, _default_value, _label)
       when is_integer(value) and value > 0,
       do: {:ok, value}

  defp resolve_positive_integer(_value, _default_value, label),
    do: {:error, "#{label} precisa ser inteiro positivo"}

  defp resolve_compression_level(nil, runtime_tuning),
    do: {:ok, default_compression_level(runtime_tuning)}

  defp resolve_compression_level(level, _runtime_tuning)
       when is_integer(level) and level >= 0 and level <= 9,
       do: {:ok, level}

  defp resolve_compression_level(_, _runtime_tuning),
    do: {:error, "compression_level precisa estar entre 0 e 9"}

  defp resolve_expected_size_bytes(options) do
    expected_size_bytes = options[:expected_size_bytes]
    expected_size_gib = options[:expected_size_gib]

    cond do
      not is_nil(expected_size_bytes) and not is_nil(expected_size_gib) ->
        {:error, "use apenas expected_size_bytes ou expected_size_gib"}

      not is_nil(expected_size_bytes) ->
        resolve_positive_integer(
          expected_size_bytes,
          @default_expected_size_bytes,
          "expected_size_bytes"
        )

      not is_nil(expected_size_gib) ->
        with {:ok, parsed_gib} <-
               resolve_positive_integer(expected_size_gib, 10, "expected_size_gib") do
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
          supports_num_parallel_collections:
            flag_supported?(help_text, "--numParallelCollections")
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
        Enum.reduce(@legacy_tls_query_to_ssl, original_query, fn {legacy_key, replacement_key},
                                                                 query ->
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
    {:ok, "#{prefix}docdb-backup-#{build_backup_timestamp()}.archive.gz"}
  end

  defp build_parallel_session_prefix(prefix) do
    {:ok, "#{prefix}docdb-backup-#{build_backup_timestamp()}/"}
  end

  defp build_backup_timestamp do
    DateTime.utc_now()
    |> DateTime.to_iso8601()
    |> String.replace([":", "-"], "")
    |> String.replace(".", "")
  end

  defp run_backup(args, capabilities) do
    case resolve_execution_plan(args) do
      {:parallel, target_databases, strategy_label} ->
        IO.puts("plano: #{strategy_label}")
        run_parallel_database_pipelines(args, capabilities, target_databases, strategy_label)

      {:single, strategy_label} ->
        IO.puts("plano: #{strategy_label}")

        with {:ok, key} <- build_s3_key(args.prefix),
             {:ok, metrics} <- run_pipeline(args, capabilities, key) do
          {:ok,
           %{
             mode: :single,
             metrics: metrics,
             destinations: ["s3://#{args.bucket}/#{key}"]
           }}
        end
    end
  end

  defp resolve_execution_plan(%{parallel_databases: true} = args) do
    case resolve_target_databases(args) do
      {:ok, [_single_database]} ->
        {:single, "single_stream (1 database explícito)"}

      {:ok, target_databases} ->
        {:parallel, target_databases, "parallel_databases (explícito)"}

      {:error, message} ->
        {:single, "single_stream (fallback: #{message})"}
    end
  end

  defp resolve_execution_plan(args) do
    cond do
      database_targeted_in_mongodump_args?(args.extra_mongodump_args) ->
        {:single, "single_stream (mongodump já segmentado por --db/--collection)"}

      true ->
        case discover_databases(args.uri, args.extra_mongodump_args) do
          {:ok, []} ->
            {:single, "single_stream (nenhum database elegível descoberto)"}

          {:ok, [_single_database]} ->
            {:single, "single_stream (1 database descoberto)"}

          {:ok, target_databases} ->
            case effective_parallel_database_concurrency(args, target_databases) do
              concurrency when concurrency > 1 ->
                {:parallel, target_databases,
                 "parallel_databases (auto: #{length(target_databases)} databases, concurrency=#{concurrency})"}

              _ ->
                {:single, "single_stream (auto: host/dados favorecem pipeline único)"}
            end

          {:error, _message} ->
            {:single, "single_stream (fallback: descoberta automática indisponível)"}
        end
    end
  end

  defp database_targeted_in_mongodump_args?(extra_mongodump_args) do
    Enum.any?(extra_mongodump_args, &parallel_database_incompatible_arg?/1)
  end

  defp print_backup_summary(outcome) do
    IO.puts("backup concluído")

    case outcome.mode do
      :single ->
        [destination] = outcome.destinations
        IO.puts("destino: #{destination}")

      :parallel_databases ->
        IO.puts("destinos:")

        outcome.destinations
        |> Enum.sort()
        |> Enum.each(&IO.puts/1)
    end
  end

  defp run_parallel_database_pipelines(args, capabilities, target_databases, strategy_label) do
    started_at = System.monotonic_time(:microsecond)

    with {:ok, target_databases} <- ensure_target_databases(args, target_databases),
         {:ok, session_prefix} <- build_parallel_session_prefix(args.prefix) do
      parallel_args = apply_parallel_database_runtime_tuning(args, target_databases)
      target_database_names = Enum.map(target_databases, & &1.name)

      print_parallel_database_config(
        parallel_args,
        capabilities,
        target_databases,
        session_prefix,
        args.expected_size_bytes,
        strategy_label
      )

      stage_specs =
        target_database_names
        |> Enum.map(fn database_name ->
          %{name: database_name, activity_label: "stream", parallelism: 1}
        end)

      progress_display = start_progress_display("backup paralelo por database", stage_specs)

      initial_state =
        build_parallel_scheduler_state(
          target_databases,
          target_database_names,
          parallel_args,
          capabilities,
          session_prefix,
          progress_display,
          args.expected_size_bytes
        )

      result =
        initial_state
        |> start_parallel_database_scheduler()
        |> Map.take([:stage_states, :destinations, :errors, :completed_metrics])

      stop_progress_display(progress_display, result.stage_states)

      total_processed_bytes =
        result.completed_metrics
        |> Kernel.++(Enum.map(result.errors, & &1.metrics))
        |> sum_reported_bytes()

      case result.errors do
        [] ->
          {:ok,
           %{
             mode: :parallel_databases,
             metrics: %{
               duration_us: System.monotonic_time(:microsecond) - started_at,
               raw_bytes: total_processed_bytes,
               estimated_bytes: args.expected_size_bytes
             },
             destinations: Enum.sort(result.destinations)
           }}

        errors ->
          {:error, format_parallel_database_errors(errors),
           %{
             duration_us: System.monotonic_time(:microsecond) - started_at,
             raw_bytes: total_processed_bytes,
             estimated_bytes: 0
           }}
      end
    end
  end

  defp ensure_target_databases(_args, target_databases) when is_list(target_databases),
    do: {:ok, target_databases}

  defp ensure_target_databases(args, nil), do: resolve_target_databases(args)

  defp resolve_target_databases(%{database_names: []} = args) do
    discover_databases(args.uri, args.extra_mongodump_args)
  end

  defp resolve_target_databases(%{database_names: database_names}) do
    {:ok, Enum.map(database_names, &%{name: &1, size_on_disk: nil})}
  end

  defp discover_databases(uri, extra_connection_args) do
    with {:ok, discovery_command} <- database_discovery_command(uri, extra_connection_args),
         {output, 0} <- run_database_discovery(discovery_command),
         {:ok, databases} <- parse_discovered_databases(output) do
      case filter_discovered_databases(databases) do
        [] -> {:error, "nenhum database de usuário encontrado para o modo --parallel-databases"}
        discovered_databases -> {:ok, discovered_databases}
      end
    else
      {:error, message} ->
        {:error, message}

      {output, status} ->
        {:error, "falha ao descobrir databases (status #{status}): #{String.trim(output)}"}
    end
  end

  defp database_discovery_command(uri, extra_connection_args) do
    script =
      "db.adminCommand({ listDatabases: 1 }).databases.forEach((item) => console.log([item.name, item.sizeOnDisk || 0].join('\\t')))"

    cond do
      System.find_executable("mongosh") ->
        discovery_uri = normalize_discovery_uri_query(uri, :mongosh)
        discovery_args = discovery_connection_args(extra_connection_args, :mongosh)
        {:ok, {"mongosh", ["--quiet", discovery_uri] ++ discovery_args ++ ["--eval", script]}}

      System.find_executable("mongo") ->
        legacy_script =
          "db.adminCommand({ listDatabases: 1 }).databases.forEach(function(item) { print(item.name + '\\t' + (item.sizeOnDisk || 0)) })"

        discovery_uri = normalize_discovery_uri_query(uri, :mongo)
        discovery_args = discovery_connection_args(extra_connection_args, :mongo)

        {:ok,
         {"mongo", ["--quiet", discovery_uri] ++ discovery_args ++ ["--eval", legacy_script]}}

      true ->
        {:error,
         "modo --parallel-databases sem --database exige mongosh ou mongo no PATH para descobrir os databases"}
    end
  end

  defp discovery_connection_args(extra_connection_args, discovery_tool) do
    extra_connection_args
    |> Enum.filter(&discovery_connection_arg?/1)
    |> Enum.map(&translate_discovery_connection_arg(&1, discovery_tool))
    |> Enum.uniq()
  end

  defp discovery_connection_arg?(arg) do
    normalized_arg = String.trim(arg)

    String.starts_with?(normalized_arg, "--tls") or
      String.starts_with?(normalized_arg, "--ssl")
  end

  defp translate_discovery_connection_arg(arg, :mongosh) do
    translate_flag_prefix(arg, @legacy_ssl_to_tls)
  end

  defp translate_discovery_connection_arg(arg, :mongo) do
    translate_flag_prefix(arg, @legacy_tls_to_ssl)
  end

  defp translate_flag_prefix(arg, replacements) do
    case String.split(arg, "=", parts: 2) do
      [flag, value] ->
        Map.get(replacements, flag, flag) <> "=" <> value

      [flag] ->
        Map.get(replacements, flag, flag)
    end
  end

  defp normalize_discovery_uri_query(uri, :mongosh) do
    normalize_uri_query_keys(uri, @legacy_ssl_query_to_tls)
  end

  defp normalize_discovery_uri_query(uri, :mongo) do
    normalize_uri_query_keys(uri, @legacy_tls_query_to_ssl)
  end

  defp normalize_uri_query_keys(uri, replacements) do
    parsed_uri = URI.parse(uri)

    if is_nil(parsed_uri.query) or parsed_uri.query == "" do
      uri
    else
      original_query = URI.decode_query(parsed_uri.query)

      normalized_query =
        Enum.reduce(replacements, original_query, fn {legacy_key, replacement_key}, query ->
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

  defp run_database_discovery({binary, command_args}) do
    System.cmd(binary, command_args, stderr_to_stdout: true)
  rescue
    error ->
      {:error, Exception.message(error)}
  end

  defp parse_discovered_databases(output) do
    databases =
      output
      |> String.split("\n", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == "" or &1 == "undefined"))
      |> Enum.map(&parse_discovered_database_line/1)
      |> Enum.reject(&is_nil/1)
      |> Enum.uniq_by(& &1.name)

    {:ok, databases}
  end

  defp parse_discovered_database_line(line) do
    case String.split(line, "\t", parts: 2) do
      [database_name, size_text] ->
        %{
          name: database_name,
          size_on_disk: parse_database_size_on_disk(size_text)
        }

      [database_name] ->
        %{
          name: database_name,
          size_on_disk: nil
        }

      _ ->
        nil
    end
  end

  defp parse_database_size_on_disk(value) do
    value
    |> String.trim()
    |> case do
      "" ->
        nil

      normalized ->
        case Integer.parse(normalized) do
          {parsed_size, ""} when parsed_size >= 0 -> parsed_size
          _ -> nil
        end
    end
  end

  defp filter_discovered_databases(databases) do
    databases
    |> Enum.reject(&(&1.name in ["admin", "config", "local"]))
    |> Enum.sort_by(fn database -> {-normalized_database_size(database), database.name} end)
  end

  defp apply_parallel_database_runtime_tuning(args, target_databases) do
    effective_database_concurrency =
      effective_parallel_database_concurrency(args, target_databases)

    %{
      args
      | database_concurrency: effective_database_concurrency,
        num_parallel_collections: parallel_database_num_parallel_collections(args),
        pigz_threads: parallel_database_pigz_threads(args)
    }
    |> distribute_parallel_database_runtime_budgets()
  end

  defp effective_parallel_database_concurrency(args, target_databases) do
    requested_concurrency =
      target_databases
      |> length()
      |> min(max(args.database_concurrency, 1))
      |> max(1)

    case requested_concurrency do
      1 ->
        1

      _ ->
        if should_reduce_parallel_database_concurrency?(target_databases) do
          1
        else
          requested_concurrency
        end
    end
  end

  defp distribute_parallel_database_runtime_budgets(args) do
    %{
      args
      | num_parallel_collections:
          distribute_parallel_budget(
            args.num_parallel_collections,
            args.database_concurrency,
            args.num_parallel_collections_source
          ),
        pigz_threads:
          distribute_parallel_budget(
            args.pigz_threads,
            args.database_concurrency,
            args.pigz_threads_source
          ),
        s3_max_concurrent_requests:
          distribute_parallel_budget(
            args.s3_max_concurrent_requests,
            args.database_concurrency,
            args.s3_max_concurrent_requests_source
          ),
        s3_max_queue_size:
          distribute_parallel_budget(
            args.s3_max_queue_size,
            args.database_concurrency,
            args.s3_max_queue_size_source
          )
    }
  end

  defp distribute_parallel_budget(total_budget, _database_concurrency, :cli), do: total_budget

  defp distribute_parallel_budget(total_budget, database_concurrency, :auto) do
    total_budget
    |> div(max(database_concurrency, 1))
    |> max(1)
  end

  defp parallel_database_num_parallel_collections(
         %{num_parallel_collections_source: :auto} = args
       ),
       do: args.num_parallel_collections

  defp parallel_database_num_parallel_collections(args), do: args.num_parallel_collections

  defp parallel_database_pigz_threads(%{pigz_threads_source: :auto} = args), do: args.pigz_threads
  defp parallel_database_pigz_threads(args), do: args.pigz_threads

  defp should_reduce_parallel_database_concurrency?(target_databases) do
    sorted_sizes =
      target_databases
      |> Enum.map(&normalized_database_size/1)
      |> Enum.sort(:desc)

    case sorted_sizes do
      [largest, second_largest | _rest] ->
        total_size = Enum.sum(sorted_sizes)
        dominance_ratio = safe_divide(largest, total_size)
        imbalance_ratio = safe_divide(largest, max(second_largest, 1))
        dominance_ratio >= 0.70 or imbalance_ratio >= 4.0

      _ ->
        false
    end
  end

  defp expected_size_bytes_for_database(expected_size_bytes, target_databases, database) do
    total_size = target_databases |> Enum.map(&normalized_database_size/1) |> Enum.sum()
    database_size = normalized_database_size(database)

    if total_size > 0 do
      expected_size_bytes
      |> Kernel.*(database_size)
      |> div(total_size)
      |> max(1_048_576)
    else
      expected_size_bytes
      |> div(max(length(target_databases), 1))
      |> max(1_048_576)
    end
  end

  defp normalized_database_size(database) do
    case Map.get(database, :size_on_disk) do
      size when is_integer(size) and size >= 0 -> size
      _ -> 0
    end
  end

  defp safe_divide(_numerator, 0), do: 0.0
  defp safe_divide(numerator, denominator), do: numerator / denominator

  defp print_parallel_database_config(
         args,
         capabilities,
         target_databases,
         session_prefix,
         expected_size_bytes,
         strategy_label
       ) do
    print_config(args, capabilities)

    IO.puts(
      "modo: #{strategy_label} database_concurrency=#{args.database_concurrency} databases=#{length(target_databases)} per_pipeline=#{format_parallel_runtime(args, capabilities)}"
    )

    IO.puts("destino-base: s3://#{args.bucket}/#{session_prefix}")
    IO.puts("expected_size_total: #{format_bytes_binary(expected_size_bytes)}")
    IO.puts("databases: #{format_target_databases(target_databases)}")
  end

  defp format_target_databases(target_databases) do
    target_databases
    |> Enum.map(fn database ->
      "#{database.name}(#{format_database_size_label(database)})"
    end)
    |> Enum.join(", ")
  end

  defp format_database_size_label(database) do
    case Map.get(database, :size_on_disk) do
      size when is_integer(size) and size >= 0 -> format_bytes_binary(size)
      _ -> "desconhecido"
    end
  end

  defp run_parallel_database_pipeline(
         args,
         capabilities,
         session_prefix,
         database,
         per_database_expected_size_bytes,
         progress_target
       ) do
    database_name = database.name
    key = build_parallel_database_key(session_prefix, database_name)

    database_args = %{
      args
      | expected_size_bytes: per_database_expected_size_bytes,
        extra_mongodump_args: args.extra_mongodump_args ++ ["--db=#{database_name}"]
    }

    case run_pipeline(database_args, capabilities, key,
           show_config?: false,
           show_progress?: false,
           print_output?: false,
           stream_progress_target: progress_target
         ) do
      {:ok, metrics} ->
        {:ok,
         %{
           database_name: database_name,
           destination: "s3://#{args.bucket}/#{key}",
           metrics: metrics
         }}

      {:error, message, metrics} ->
        {:error, %{database_name: database_name, message: message, metrics: metrics}}
    end
  end

  defp build_parallel_scheduler_state(
         target_databases,
         target_database_names,
         parallel_args,
         capabilities,
         session_prefix,
         progress_display,
         expected_size_bytes
       ) do
    pending_work =
      Enum.map(target_databases, fn database ->
        %{
          database: database,
          expected_size_bytes:
            expected_size_bytes_for_database(expected_size_bytes, target_databases, database)
        }
      end)

    %{
      pending_work: pending_work,
      running_tasks: %{},
      current_concurrency: parallel_args.database_concurrency,
      stage_states:
        Map.new(target_database_names, fn database_name ->
          {database_name, {:running, nil, "aguardando agendamento"}}
        end),
      destinations: [],
      errors: [],
      completed_metrics: [],
      live_metrics: %{},
      total_expected_bytes: expected_size_bytes,
      progress_display: progress_display,
      parallel_args: parallel_args,
      capabilities: capabilities,
      session_prefix: session_prefix
    }
  end

  defp start_parallel_database_scheduler(state) do
    state
    |> update_parallel_progress_summary()
    |> fill_parallel_database_workers()
    |> parallel_database_scheduler_loop()
  end

  defp fill_parallel_database_workers(state) do
    cond do
      map_size(state.running_tasks) >= state.current_concurrency ->
        state

      state.pending_work == [] ->
        state

      true ->
        [work_item | remaining_work] = state.pending_work
        task = spawn_parallel_database_task(state, work_item)

        running_tasks =
          Map.put(state.running_tasks, task.ref, %{task: task, work_item: work_item})

        stage_state =
          {:running, nil, "alvo=#{format_bytes_binary(work_item.expected_size_bytes)}"}

        updated_state = %{
          state
          | pending_work: remaining_work,
            running_tasks: running_tasks,
            stage_states: Map.put(state.stage_states, work_item.database.name, stage_state)
        }

        update_progress_display(
          updated_state.progress_display,
          work_item.database.name,
          stage_state
        )

        updated_state
        |> register_live_metric(
          work_item.database.name,
          build_stream_progress_snapshot(
            0,
            System.monotonic_time(:microsecond),
            work_item.expected_size_bytes
          )
        )
        |> fill_parallel_database_workers()
    end
  end

  defp spawn_parallel_database_task(state, work_item) do
    scheduler_pid = self()

    Task.async(fn ->
      run_parallel_database_pipeline(
        state.parallel_args,
        state.capabilities,
        state.session_prefix,
        work_item.database,
        work_item.expected_size_bytes,
        [
          {:display_stage, state.progress_display, work_item.database.name},
          {:scheduler, scheduler_pid, work_item.database.name}
        ]
      )
    end)
  end

  defp parallel_database_scheduler_loop(%{pending_work: [], running_tasks: running_tasks} = state)
       when map_size(running_tasks) == 0,
       do: state

  defp parallel_database_scheduler_loop(state) do
    receive do
      {:stream_progress, database_name, snapshot} ->
        state
        |> register_live_metric(database_name, snapshot)
        |> parallel_database_scheduler_loop()

      {ref, task_result} ->
        handle_parallel_database_task_result(state, ref, task_result)

      {:DOWN, ref, :process, _pid, reason} ->
        handle_parallel_database_task_down(state, ref, reason)
    end
  end

  defp handle_parallel_database_task_result(state, ref, task_result) do
    case Map.pop(state.running_tasks, ref) do
      {nil, _running_tasks} ->
        parallel_database_scheduler_loop(state)

      {%{task: task, work_item: work_item}, running_tasks} ->
        Process.demonitor(task.ref, [:flush])

        state
        |> Map.put(:running_tasks, running_tasks)
        |> apply_parallel_database_task_result(work_item, task_result)
        |> maybe_reduce_runtime_database_concurrency()
        |> fill_parallel_database_workers()
        |> parallel_database_scheduler_loop()
    end
  end

  defp handle_parallel_database_task_down(state, ref, reason) do
    case Map.pop(state.running_tasks, ref) do
      {nil, _running_tasks} ->
        parallel_database_scheduler_loop(state)

      {%{work_item: work_item}, running_tasks} ->
        error =
          %{
            database_name: work_item.database.name,
            message: "task abortada: #{inspect(reason)}",
            metrics: %{
              duration_us: 0,
              raw_bytes: 0,
              estimated_bytes: work_item.expected_size_bytes
            }
          }

        state
        |> Map.put(:running_tasks, running_tasks)
        |> register_parallel_database_error(error)
        |> maybe_reduce_runtime_database_concurrency()
        |> fill_parallel_database_workers()
        |> parallel_database_scheduler_loop()
    end
  end

  defp apply_parallel_database_task_result(state, _work_item, {:ok, result}) do
    state
    |> register_parallel_database_success(result)
  end

  defp apply_parallel_database_task_result(state, _work_item, {:error, error}) do
    state
    |> register_parallel_database_error(error)
  end

  defp register_parallel_database_success(state, result) do
    throughput_label = format_parallel_database_completion_label(result.metrics)
    stage_state = {:done, 0, throughput_label}

    update_progress_display(state.progress_display, result.database_name, stage_state)

    state
    |> drop_live_metric(result.database_name)
    |> then(fn updated_state ->
      %{
        updated_state
        | stage_states: Map.put(updated_state.stage_states, result.database_name, stage_state),
          destinations: [result.destination | updated_state.destinations],
          completed_metrics: [result.metrics | updated_state.completed_metrics]
      }
    end)
  end

  defp register_parallel_database_error(state, error) do
    stage_state = {:failed, 1, "sem throughput útil"}

    update_progress_display(state.progress_display, error.database_name, stage_state)

    state
    |> drop_live_metric(error.database_name)
    |> then(fn updated_state ->
      %{
        updated_state
        | stage_states: Map.put(updated_state.stage_states, error.database_name, stage_state),
          errors: [error | updated_state.errors]
      }
    end)
  end

  defp maybe_reduce_runtime_database_concurrency(
         %{current_concurrency: current_concurrency} = state
       )
       when current_concurrency <= 1,
       do: state

  defp maybe_reduce_runtime_database_concurrency(state) do
    if should_step_down_runtime_parallelism?(state) do
      new_concurrency = max(state.current_concurrency - 1, 1)

      IO.puts(
        "ajuste dinâmico: database_concurrency #{state.current_concurrency} -> #{new_concurrency} por throughput observado"
      )

      %{state | current_concurrency: new_concurrency}
    else
      state
    end
  end

  defp should_step_down_runtime_parallelism?(state) do
    completed_count = length(state.completed_metrics)
    tuning_profile = state.parallel_args.runtime_tuning.tuning_profile

    completed_count >= state.current_concurrency and
      state.pending_work != [] and
      tuning_profile in [:conservative, :cpu_limited_throughput, :cpu_limited_balanced] and
      average_observed_throughput_mib_per_sec(state.completed_metrics) <
        throughput_floor_mib_per_sec(tuning_profile)
  end

  defp average_observed_throughput_mib_per_sec([]), do: 0.0

  defp average_observed_throughput_mib_per_sec(completed_metrics) do
    completed_metrics
    |> Enum.map(&reported_throughput_mib_per_sec/1)
    |> Enum.sum()
    |> Kernel./(length(completed_metrics))
  end

  defp register_live_metric(state, database_name, snapshot) do
    state
    |> put_in([:live_metrics, database_name], snapshot)
    |> update_parallel_progress_summary()
  end

  defp drop_live_metric(state, database_name) do
    state
    |> update_in([:live_metrics], &Map.delete(&1, database_name))
    |> update_parallel_progress_summary()
  end

  defp update_parallel_progress_summary(%{progress_display: nil} = state), do: state

  defp update_parallel_progress_summary(state) do
    update_progress_summary(state.progress_display, parallel_progress_summary(state))
    state
  end

  defp parallel_progress_summary(state) do
    completed_bytes = sum_reported_bytes(state.completed_metrics)
    active_bytes = sum_live_bytes(state.live_metrics)
    processed_bytes = completed_bytes + active_bytes
    active_rate_mib_per_sec = sum_live_throughput(state.live_metrics)
    remaining_bytes = max(state.total_expected_bytes - processed_bytes, 0)

    %{
      aggregate:
        "agregado: #{format_bytes_binary(processed_bytes)}/#{format_bytes_binary(state.total_expected_bytes)} @ #{format_mib_per_sec(active_rate_mib_per_sec)} | ativos=#{map_size(state.live_metrics)}#{format_eta_detail(remaining_bytes, active_rate_mib_per_sec)}",
      active: "por database: #{format_active_database_rates(state.live_metrics)}"
    }
  end

  defp sum_live_bytes(live_metrics) do
    live_metrics
    |> Map.values()
    |> Enum.map(&Map.get(&1, :raw_bytes, 0))
    |> Enum.sum()
  end

  defp sum_live_throughput(live_metrics) do
    live_metrics
    |> Map.values()
    |> Enum.map(&Map.get(&1, :throughput_mib_per_sec, 0.0))
    |> Enum.sum()
  end

  defp sum_reported_bytes(metrics_list) do
    metrics_list
    |> Enum.map(fn metrics ->
      raw_bytes = Map.get(metrics, :raw_bytes, 0)

      if raw_bytes > 0 do
        raw_bytes
      else
        Map.get(metrics, :estimated_bytes, 0)
      end
    end)
    |> Enum.sum()
  end

  defp format_active_database_rates(live_metrics) when map_size(live_metrics) == 0,
    do: "nenhum ativo"

  defp format_active_database_rates(live_metrics) do
    {visible_entries, hidden_entries} =
      live_metrics
      |> Enum.sort_by(
        fn {_database_name, snapshot} -> Map.get(snapshot, :throughput_mib_per_sec, 0.0) end,
        :desc
      )
      |> Enum.split(4)

    visible_text =
      visible_entries
      |> Enum.map(fn {database_name, snapshot} ->
        "#{database_name}=#{format_mib_per_sec(Map.get(snapshot, :throughput_mib_per_sec, 0.0))}"
      end)
      |> Enum.join(", ")

    case hidden_entries do
      [] -> visible_text
      _ -> visible_text <> " +" <> Integer.to_string(length(hidden_entries))
    end
  end

  defp format_mib_per_sec(value) when is_integer(value), do: format_mib_per_sec(value * 1.0)
  defp format_mib_per_sec(value), do: :erlang.float_to_binary(value, decimals: 1) <> " MiB/s"

  defp format_eta_detail(_remaining_bytes, active_rate_mib_per_sec)
       when active_rate_mib_per_sec <= 0, do: ""

  defp format_eta_detail(remaining_bytes, active_rate_mib_per_sec) do
    seconds =
      remaining_bytes
      |> Kernel./(1024 * 1024)
      |> Kernel./(active_rate_mib_per_sec)
      |> round()

    " | eta=#{format_eta_seconds(seconds)}"
  end

  defp format_eta_seconds(seconds) when seconds < 60, do: "#{seconds}s"

  defp format_eta_seconds(seconds) when seconds < 3600 do
    minutes = div(seconds, 60)
    remaining_seconds = rem(seconds, 60)
    "#{minutes}m#{remaining_seconds}s"
  end

  defp format_eta_seconds(seconds) do
    hours = div(seconds, 3600)
    minutes = div(rem(seconds, 3600), 60)
    remaining_seconds = rem(seconds, 60)
    "#{hours}h#{minutes}m#{remaining_seconds}s"
  end

  defp reported_throughput_mib_per_sec(metrics) do
    if Map.get(metrics, :raw_bytes, 0) > 0 do
      measured_throughput_mib_per_sec(metrics)
    else
      estimated_throughput_mib_per_sec(metrics)
    end
  end

  defp estimated_throughput_mib_per_sec(metrics) do
    duration_seconds =
      metrics
      |> Map.get(:duration_us, 0)
      |> Kernel./(1_000_000)
      |> max(1.0)

    metrics
    |> Map.get(:estimated_bytes, 0)
    |> Kernel./(1024 * 1024)
    |> Kernel./(duration_seconds)
  end

  defp throughput_floor_mib_per_sec(:conservative), do: 20.0
  defp throughput_floor_mib_per_sec(:cpu_limited_throughput), do: 35.0
  defp throughput_floor_mib_per_sec(:cpu_limited_balanced), do: 30.0
  defp throughput_floor_mib_per_sec(:balanced), do: 45.0
  defp throughput_floor_mib_per_sec(:throughput), do: 60.0

  defp format_parallel_database_completion_label(metrics) do
    measured_bytes = Map.get(metrics, :raw_bytes, 0)
    throughput = measured_throughput_mib_per_sec(metrics)

    cond do
      measured_bytes > 0 ->
        "#{format_bytes_binary(measured_bytes)} real @ #{:erlang.float_to_binary(throughput, decimals: 1)} MiB/s"

      true ->
        estimated_bytes = Map.get(metrics, :estimated_bytes, 0)

        "#{format_bytes_binary(estimated_bytes)} estimado @ #{:erlang.float_to_binary(estimated_throughput_mib_per_sec(metrics), decimals: 1)} MiB/s"
    end
  end

  defp measured_throughput_mib_per_sec(metrics) do
    duration_seconds =
      metrics
      |> Map.get(:duration_us, 0)
      |> Kernel./(1_000_000)
      |> max(1.0)

    metrics
    |> Map.get(:raw_bytes, 0)
    |> Kernel./(1024 * 1024)
    |> Kernel./(duration_seconds)
  end

  defp build_parallel_database_key(session_prefix, database_name) do
    "#{session_prefix}#{sanitize_s3_segment(database_name)}.archive.gz"
  end

  defp sanitize_s3_segment(segment) do
    segment
    |> String.trim()
    |> String.replace(~r{[^a-zA-Z0-9._-]+}, "_")
  end

  defp format_parallel_database_errors(errors) do
    errors
    |> Enum.reverse()
    |> Enum.map(fn error ->
      """
      database: #{error.database_name}
      #{error.message}
      """
      |> String.trim()
    end)
    |> Enum.join("\n\n")
  end

  defp run_pipeline(args, capabilities, key, opts \\ []) do
    started_at = System.monotonic_time(:microsecond)
    destination = "s3://#{args.bucket}/#{key}"
    meter_progress_file = build_runtime_probe_path("stream-progress")

    mongodump_args =
      ["mongodump", "--uri", args.uri, "--archive"]
      |> Kernel.++(num_parallel_collections_flag(args.num_parallel_collections, capabilities))
      |> Kernel.++(args.extra_mongodump_args)

    pigz_args = [
      "pigz",
      "-c",
      "-#{args.compression_level}",
      "-p",
      Integer.to_string(args.pigz_threads)
    ]

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
    : > #{shell_escape(meter_progress_file)}
    stream_meter_aws() {
      LC_ALL=C dd bs=#{args.meter_block_size_mib}M of=/dev/stdout status=progress 2>#{shell_escape(meter_progress_file)} | #{aws_command} 2>"${stderr_aws}"
      meter_pipeline_status="${PIPESTATUS[*]}"
      meter_pipeline_exit=0
      for status_code in ${meter_pipeline_status}; do
        if [ "$status_code" != "0" ]; then
          meter_pipeline_exit=1
          break
        fi
      done
      return "${meter_pipeline_exit}"
    }
    #{mongodump_command} 2>"${stderr_mongodump}" | #{pigz_command} 2>"${stderr_pigz}" | stream_meter_aws
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

    show_config? = Keyword.get(opts, :show_config?, true)
    show_progress? = Keyword.get(opts, :show_progress?, true)
    print_output? = Keyword.get(opts, :print_output?, true)

    with {:ok, aws_config_path} <- write_aws_cli_s3_config(args) do
      if show_config? do
        print_config(args, capabilities)
        IO.puts("destino: #{destination}")

        IO.puts(
          "alvo: #{format_bytes_binary(args.expected_size_bytes)} em até #{@default_target_duration_seconds}s"
        )
      end

      progress_display =
        if show_progress? do
          start_progress_display("backup em andamento", progress_stage_specs(args, capabilities))
        else
          nil
        end

      stream_progress_target = resolve_stream_progress_target(opts, progress_display)

      stream_progress_watcher =
        start_stream_progress_watcher(
          meter_progress_file,
          stream_progress_target,
          started_at,
          args.expected_size_bytes
        )

      system_cmd_opts = [
        stderr_to_stdout: true,
        env: [{"AWS_CONFIG_FILE", aws_config_path}]
      ]

      result =
        case System.cmd("bash", ["-c", command], system_cmd_opts) do
          {output, 0} ->
            measured_bytes = read_streamed_bytes(meter_progress_file)
            stop_stream_progress_watcher(stream_progress_watcher)
            maybe_stop_progress_display(progress_display, success_stage_states())

            if print_output? do
              print_pipeline_output(output, status_probe, stderr_markers)
            end

            {:ok,
             %{
               duration_us: System.monotonic_time(:microsecond) - started_at,
               raw_bytes: measured_bytes,
               estimated_bytes: args.expected_size_bytes
             }}

          {output, status} ->
            measured_bytes = read_streamed_bytes(meter_progress_file)
            stop_stream_progress_watcher(stream_progress_watcher)
            pipeline_status = extract_pipeline_status(output, status_probe)
            maybe_stop_progress_display(progress_display, final_stage_states(pipeline_status))
            cleaned_output = remove_probe_sections(output, status_probe, stderr_markers)
            failed_stages = failed_pipeline_stages(pipeline_status)
            stderr_sections = extract_stderr_sections(output, stderr_markers)

            details =
              [
                "pipeline falhou com código #{status}",
                format_pipeline_stage_details(failed_stages),
                format_failed_commands(pipeline_summary, failed_stages),
                format_stage_stderr(stderr_sections, failed_stages),
                format_failure_hint(stderr_sections, failed_stages, args, capabilities),
                format_pipeline_output(cleaned_output)
              ]
              |> Enum.reject(&(&1 == ""))
              |> Enum.join("\n")

            {:error, details,
             %{
               duration_us: System.monotonic_time(:microsecond) - started_at,
               raw_bytes: measured_bytes,
               estimated_bytes: 0
             }}
        end

      cleanup_runtime_probe_file(meter_progress_file)
      cleanup_runtime_probe_file(aws_config_path)
      result
    end
  end

  defp build_runtime_probe_path(label) do
    Path.join(
      System.tmp_dir!(),
      "#{label}-#{System.system_time(:microsecond)}-#{System.unique_integer([:positive])}.log"
    )
  end

  defp write_aws_cli_s3_config(args) do
    aws_config_path = build_runtime_probe_path("aws-config")
    active_profile = System.get_env("AWS_PROFILE") || "default"
    config_prefix = aws_cli_profile_prefix(active_profile)

    base_config_path =
      System.get_env("AWS_CONFIG_FILE") || Path.join(System.user_home!(), ".aws/config")

    case copy_base_aws_config(base_config_path, aws_config_path) do
      :ok ->
        result =
          [
            {"#{config_prefix}.s3.max_concurrent_requests",
             Integer.to_string(args.s3_max_concurrent_requests)},
            {"#{config_prefix}.s3.max_queue_size", Integer.to_string(args.s3_max_queue_size)},
            {"#{config_prefix}.s3.multipart_threshold", "#{args.s3_multipart_chunksize_mib}MB"},
            {"#{config_prefix}.s3.multipart_chunksize", "#{args.s3_multipart_chunksize_mib}MB"}
          ]
          |> Enum.reduce_while({:ok, aws_config_path}, fn {key, value}, {:ok, path} ->
            case System.cmd(
                   "aws",
                   ["configure", "set", key, value],
                   stderr_to_stdout: true,
                   env: [{"AWS_CONFIG_FILE", path}]
                 ) do
              {_output, 0} ->
                {:cont, {:ok, path}}

              {output, status} ->
                {:halt,
                 {:error,
                  "falha ao preparar AWS CLI para upload multipart otimizado (status #{status}): #{String.trim(output)}"}}
            end
          end)

        case result do
          {:ok, path} ->
            {:ok, path}

          {:error, message} ->
            cleanup_runtime_probe_file(aws_config_path)
            {:error, message}
        end

      {:error, reason} ->
        {:error, "falha ao preparar configuração temporária do AWS CLI: #{inspect(reason)}"}
    end
  end

  defp aws_cli_profile_prefix("default"), do: "default"
  defp aws_cli_profile_prefix(profile_name), do: "profile.#{profile_name}"

  defp copy_base_aws_config(base_config_path, aws_config_path) do
    case File.read(base_config_path) do
      {:ok, config_content} -> File.write(aws_config_path, config_content)
      {:error, :enoent} -> File.write(aws_config_path, "")
      {:error, reason} -> {:error, reason}
    end
  end

  defp cleanup_runtime_probe_file(path) do
    File.rm(path)
    :ok
  end

  defp resolve_stream_progress_target(opts, progress_display) do
    case Keyword.get(opts, :stream_progress_target) do
      nil ->
        if is_pid(progress_display) do
          {:display_stage, progress_display, "aws"}
        else
          nil
        end

      explicit_target ->
        explicit_target
    end
  end

  defp start_stream_progress_watcher(_meter_progress_file, nil, _started_at, _target_bytes),
    do: nil

  defp start_stream_progress_watcher(
         meter_progress_file,
         progress_target,
         started_at,
         target_bytes
       ) do
    spawn(fn ->
      stream_progress_watcher_loop(
        meter_progress_file,
        progress_target,
        started_at,
        target_bytes,
        0
      )
    end)
  end

  defp stop_stream_progress_watcher(nil), do: :ok

  defp stop_stream_progress_watcher(pid) do
    send(pid, :stop)
    :ok
  end

  defp stream_progress_watcher_loop(
         meter_progress_file,
         progress_target,
         started_at,
         target_bytes,
         last_bytes
       ) do
    receive do
      :stop ->
        :ok
    after
      1_000 ->
        current_bytes = read_streamed_bytes(meter_progress_file)

        if current_bytes > last_bytes do
          snapshot = build_stream_progress_snapshot(current_bytes, started_at, target_bytes)
          publish_stream_progress(progress_target, snapshot)
        end

        stream_progress_watcher_loop(
          meter_progress_file,
          progress_target,
          started_at,
          target_bytes,
          max(current_bytes, last_bytes)
        )
    end
  end

  defp publish_stream_progress(targets, snapshot) when is_list(targets) do
    Enum.each(targets, &publish_stream_progress(&1, snapshot))
  end

  defp publish_stream_progress({:display_stage, progress_display, stage_name}, snapshot) do
    update_progress_display(progress_display, stage_name, {:running, nil, snapshot.detail})
  end

  defp publish_stream_progress({:scheduler, scheduler_pid, database_name}, snapshot) do
    send(scheduler_pid, {:stream_progress, database_name, snapshot})
    :ok
  end

  defp publish_stream_progress(_, _snapshot), do: :ok

  defp build_stream_progress_snapshot(streamed_bytes, started_at, target_bytes) do
    elapsed_seconds =
      System.monotonic_time(:microsecond)
      |> Kernel.-(started_at)
      |> Kernel./(1_000_000)
      |> max(1.0)

    throughput =
      streamed_bytes
      |> Kernel./(1024 * 1024)
      |> Kernel./(elapsed_seconds)

    %{
      raw_bytes: streamed_bytes,
      throughput_mib_per_sec: throughput,
      target_bytes: target_bytes,
      detail:
        "#{format_bytes_binary(streamed_bytes)} real @ #{format_mib_per_sec(throughput)}#{format_eta_detail(max(target_bytes - streamed_bytes, 0), throughput)}"
    }
  end

  defp read_streamed_bytes(meter_progress_file) do
    case File.read(meter_progress_file) do
      {:ok, contents} ->
        contents
        |> Regex.scan(~r/(\d+)\s+bytes\b/)
        |> List.last()
        |> case do
          [_, bytes_text] ->
            case Integer.parse(bytes_text) do
              {bytes, ""} -> bytes
              _ -> 0
            end

          _ ->
            0
        end

      _ ->
        0
    end
  end

  defp num_parallel_collections_flag(_num_parallel_collections, %{
         supports_num_parallel_collections: false
       }),
       do: []

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

  defp format_failure_hint(stderr_sections, failed_stages, args, capabilities) do
    mongodump_failed? = Enum.any?(failed_stages, &String.starts_with?(&1, "mongodump="))
    mongodump_stderr = Map.get(stderr_sections, "mongodump", "")
    normalized_stderr = String.downcase(mongodump_stderr)

    cond do
      not mongodump_failed? ->
        ""

      String.contains?(normalized_stderr, "low available memory") ->
        [
          "dica: o mongodump encerrou por falta de memória.",
          "ajuste aplicado: os defaults agora usam auto-tuning por RAM/CPU do host.",
          "configuração atual: #{format_parallel_runtime(args, capabilities)}",
          "se precisar forçar modo conservador, rode com: --num-parallel-collections 1 --pigz-threads 1"
        ]
        |> Enum.join("\n")

      true ->
        ""
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
    runtime_tuning = Map.get(args, :runtime_tuning, default_runtime_tuning())

    num_parallel_display =
      if capabilities.supports_num_parallel_collections do
        Integer.to_string(args.num_parallel_collections)
      else
        "desativado"
      end

    IO.puts(
      "host: schedulers=#{runtime_tuning.schedulers_online} mem_available=#{format_memory_available(runtime_tuning.mem_available_bytes)} tuning_profile=#{runtime_tuning.tuning_profile}"
    )

    IO.puts(
      "config: numParallelCollections=#{num_parallel_display} (#{Map.get(args, :num_parallel_collections_source, :auto)}) pigz_threads=#{args.pigz_threads} (#{Map.get(args, :pigz_threads_source, :auto)}) compression_level=#{args.compression_level} (#{Map.get(args, :compression_level_source, :auto)}) s3_max_concurrent_requests=#{args.s3_max_concurrent_requests} (#{Map.get(args, :s3_max_concurrent_requests_source, :auto)}) s3_max_queue_size=#{args.s3_max_queue_size} (#{Map.get(args, :s3_max_queue_size_source, :auto)}) s3_multipart_chunksize=#{args.s3_multipart_chunksize_mib}MiB (#{Map.get(args, :s3_multipart_chunksize_mib_source, :auto)}) meter_block_size=#{args.meter_block_size_mib}MiB (#{Map.get(args, :meter_block_size_mib_source, :auto)}) expected_size=#{format_bytes_binary(args.expected_size_bytes)}"
    )
  end

  defp option_source(nil), do: :auto
  defp option_source(_value), do: :cli

  defp format_parallel_runtime(args, capabilities) do
    num_parallel_display =
      if capabilities.supports_num_parallel_collections do
        Integer.to_string(args.num_parallel_collections)
      else
        "desativado"
      end

    "numParallelCollections=#{num_parallel_display} pigz_threads=#{args.pigz_threads} compression_level=#{args.compression_level} s3_max_concurrent_requests=#{args.s3_max_concurrent_requests} s3_max_queue_size=#{args.s3_max_queue_size} s3_multipart_chunksize=#{args.s3_multipart_chunksize_mib}MiB"
  end

  defp print_performance_report(metrics, expected_size_bytes) do
    duration_us = Map.get(metrics, :duration_us, 0)
    duration_seconds = max(1, div(duration_us, 1_000_000))
    raw_bytes = Map.get(metrics, :raw_bytes, 0)
    estimated_bytes = Map.get(metrics, :estimated_bytes, 0)

    IO.puts("tempo total: #{format_duration(duration_us)}")

    if raw_bytes > 0 do
      throughput = raw_bytes / 1024.0 / 1024.0 / duration_seconds

      IO.puts(
        "volume processado: #{format_bytes_binary(raw_bytes)} (~#{:erlang.float_to_binary(throughput, decimals: 2)} MiB/s)"
      )
    else
      if estimated_bytes > 0 do
        throughput = estimated_bytes / 1024.0 / 1024.0 / duration_seconds

        IO.puts(
          "volume estimado: #{format_bytes_binary(estimated_bytes)} (~#{:erlang.float_to_binary(throughput, decimals: 2)} MiB/s)"
        )
      else
        IO.puts("volume processado: sem bytes (não foi possível mensurar)")
      end
    end

    target_duration_seconds = @default_target_duration_seconds
    target_speed_mib_per_sec = expected_size_bytes / 1024.0 / 1024.0 / target_duration_seconds

    target_gib_per_min =
      expected_size_bytes / 1024.0 / 1024.0 / 1024.0 / (target_duration_seconds / 60.0)

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

  defp do_format_bytes_binary(value, [_unit | rest]),
    do: do_format_bytes_binary(value / 1024.0, rest)

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

  defp format_memory_available(nil), do: "desconhecida"
  defp format_memory_available(bytes), do: format_bytes_binary(bytes)

  defp success_stage_states do
    %{
      "mongodump" => {:done, 0},
      "pigz" => {:done, 0},
      "aws" => {:done, 0}
    }
  end

  defp progress_stage_specs(args, capabilities) do
    mongodump_parallelism =
      if capabilities.supports_num_parallel_collections do
        max(args.num_parallel_collections, 1)
      else
        1
      end

    [
      %{name: "mongodump", activity_label: "workers", parallelism: mongodump_parallelism},
      %{name: "pigz", activity_label: "threads", parallelism: max(args.pigz_threads, 1)},
      %{name: "aws", activity_label: "reqs", parallelism: max(args.s3_max_concurrent_requests, 1)}
    ]
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

  defp start_progress_display(message, stage_specs) do
    ansi_enabled = IO.ANSI.enabled?()
    stage_names = Enum.map(stage_specs, & &1.name)

    initial_state = %{
      message: message,
      stage_specs: stage_specs,
      stage_states: Map.new(stage_names, &{&1, {:running, nil, ""}}),
      summary: nil,
      started_at: System.monotonic_time(:millisecond),
      frame: 0,
      ansi_enabled: ansi_enabled,
      first_render?: true,
      rendered_line_count: 0
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

      {:update, stage_name, stage_state} ->
        state
        |> put_in([:stage_states, stage_name], stage_state)
        |> render_progress_display()
        |> progress_display_loop()

      {:summary, summary} ->
        state
        |> Map.put(:summary, summary)
        |> render_progress_display()
        |> progress_display_loop()
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

  defp maybe_stop_progress_display(nil, _stage_states), do: :ok

  defp maybe_stop_progress_display(pid, stage_states),
    do: stop_progress_display(pid, stage_states)

  defp update_progress_display(nil, _stage_name, _stage_state), do: :ok

  defp update_progress_display(pid, stage_name, stage_state) do
    send(pid, {:update, stage_name, stage_state})
    :ok
  end

  defp update_progress_summary(nil, _summary), do: :ok

  defp update_progress_summary(pid, summary) do
    send(pid, {:summary, summary})
    :ok
  end

  defp render_progress_display(state) do
    lines = build_progress_lines(state)

    if state.ansi_enabled do
      if state.first_render? do
        IO.write(Enum.join(lines, "\n") <> "\n")
      else
        IO.write(IO.ANSI.cursor_up(state.rendered_line_count))

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

    %{state | first_render?: false, rendered_line_count: length(lines)}
  end

  defp build_progress_lines(state) do
    elapsed_seconds = div(System.monotonic_time(:millisecond) - state.started_at, 1000)

    ["#{state.message} (#{elapsed_seconds}s)"]
    |> Kernel.++(format_progress_summary_lines(Map.get(state, :summary)))
    |> Kernel.++(
      Enum.map(state.stage_specs, fn stage_spec ->
        format_progress_stage_line(
          stage_spec,
          Map.get(state.stage_states, stage_spec.name, {:running, nil, ""}),
          state.frame
        )
      end)
    )
  end

  defp format_progress_summary_lines(nil), do: []

  defp format_progress_summary_lines(summary) do
    [Map.get(summary, :aggregate), Map.get(summary, :active)]
    |> Enum.reject(&(&1 in [nil, ""]))
  end

  defp format_progress_stage_line(stage_spec, {:running, _status} = stage_state, frame) do
    "#{String.pad_trailing(stage_spec.name, 10)} #{indeterminate_bar(frame, 20)} running | #{format_stage_parallelism(stage_spec, stage_state, frame)}"
  end

  defp format_progress_stage_line(stage_spec, {:running, _status, detail} = stage_state, frame) do
    "#{String.pad_trailing(stage_spec.name, 10)} #{indeterminate_bar(frame, 20)} running | #{format_stage_parallelism(stage_spec, stage_state, frame)}#{format_stage_detail(detail)}"
  end

  defp format_progress_stage_line(stage_spec, {:done, status} = stage_state, frame) do
    "#{String.pad_trailing(stage_spec.name, 10)} #{String.duplicate("#", 20)} done (#{status}) | #{format_stage_parallelism(stage_spec, stage_state, frame)}"
  end

  defp format_progress_stage_line(stage_spec, {:done, status, detail} = stage_state, frame) do
    "#{String.pad_trailing(stage_spec.name, 10)} #{String.duplicate("#", 20)} done (#{status}) | #{format_stage_parallelism(stage_spec, stage_state, frame)}#{format_stage_detail(detail)}"
  end

  defp format_progress_stage_line(stage_spec, {:failed, status} = stage_state, frame) do
    "#{String.pad_trailing(stage_spec.name, 10)} #{String.duplicate("!", 20)} failed (#{status}) | #{format_stage_parallelism(stage_spec, stage_state, frame)}"
  end

  defp format_progress_stage_line(stage_spec, {:failed, status, detail} = stage_state, frame) do
    "#{String.pad_trailing(stage_spec.name, 10)} #{String.duplicate("!", 20)} failed (#{status}) | #{format_stage_parallelism(stage_spec, stage_state, frame)}#{format_stage_detail(detail)}"
  end

  defp format_stage_parallelism(stage_spec, stage_state, frame) do
    visible_slots = min(max(stage_spec.parallelism, 1), 12)
    slot_indicator = format_parallelism_slots(visible_slots, stage_state, frame)
    "#{stage_spec.activity_label}=#{slot_indicator} x#{stage_spec.parallelism}"
  end

  defp format_parallelism_slots(slot_count, {:running, _status}, frame) do
    0..(slot_count - 1)
    |> Enum.map(fn index ->
      if rem(frame + index, 4) < 2 do
        "#"
      else
        "."
      end
    end)
    |> Enum.join()
  end

  defp format_parallelism_slots(slot_count, {:running, _status, _detail}, frame) do
    format_parallelism_slots(slot_count, {:running, nil}, frame)
  end

  defp format_parallelism_slots(slot_count, {:done, _status}, _frame) do
    String.duplicate("#", slot_count)
  end

  defp format_parallelism_slots(slot_count, {:done, _status, _detail}, _frame) do
    String.duplicate("#", slot_count)
  end

  defp format_parallelism_slots(slot_count, {:failed, _status}, _frame) do
    String.duplicate("!", slot_count)
  end

  defp format_parallelism_slots(slot_count, {:failed, _status, _detail}, _frame) do
    String.duplicate("!", slot_count)
  end

  defp format_stage_detail(""), do: ""
  defp format_stage_detail(detail), do: " | " <> detail

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

  defp default_runtime_tuning do
    schedulers_online = System.schedulers_online()
    mem_available_bytes = read_mem_available_bytes()
    tuning_profile = infer_tuning_profile(schedulers_online, mem_available_bytes)

    %{
      schedulers_online: schedulers_online,
      mem_available_bytes: mem_available_bytes,
      tuning_profile: tuning_profile,
      num_parallel_collections:
        recommended_num_parallel_collections(
          schedulers_online,
          mem_available_bytes,
          tuning_profile
        ),
      pigz_threads:
        recommended_pigz_threads(schedulers_online, mem_available_bytes, tuning_profile),
      compression_level: recommended_compression_level(tuning_profile),
      s3_max_concurrent_requests:
        recommended_s3_max_concurrent_requests(
          schedulers_online,
          mem_available_bytes,
          tuning_profile
        ),
      s3_max_queue_size:
        recommended_s3_max_queue_size(schedulers_online, mem_available_bytes, tuning_profile),
      s3_multipart_chunksize_mib:
        recommended_s3_multipart_chunksize_mib(
          schedulers_online,
          mem_available_bytes,
          tuning_profile
        ),
      meter_block_size_mib:
        recommended_meter_block_size_mib(schedulers_online, mem_available_bytes, tuning_profile)
    }
  end

  defp read_mem_available_bytes do
    with {:ok, meminfo} <- File.read("/proc/meminfo"),
         [value_kib] <-
           Regex.run(~r/^MemAvailable:\s+(\d+)\s+kB$/m, meminfo, capture: :all_but_first),
         {parsed_kib, ""} <- Integer.parse(value_kib) do
      parsed_kib * 1024
    else
      _ -> nil
    end
  end

  defp infer_tuning_profile(schedulers_online, nil) when schedulers_online <= 2,
    do: :cpu_limited_balanced

  defp infer_tuning_profile(_schedulers_online, nil), do: :balanced

  defp infer_tuning_profile(schedulers_online, mem_available_bytes) do
    cond do
      mem_available_bytes < gib(2) ->
        :conservative

      schedulers_online <= 2 and mem_available_bytes < gib(3) ->
        :cpu_limited_throughput

      mem_available_bytes < gib(4) ->
        :cpu_limited_balanced

      mem_available_bytes < gib(8) ->
        :balanced

      true ->
        :throughput
    end
  end

  defp default_database_concurrency(runtime_tuning) do
    case runtime_tuning.tuning_profile do
      :conservative ->
        1

      :cpu_limited_throughput ->
        min(runtime_tuning.schedulers_online, 2)

      :cpu_limited_balanced ->
        1

      :balanced ->
        min(runtime_tuning.schedulers_online, 2)

      :throughput ->
        min(runtime_tuning.schedulers_online, 4)
    end
  end

  defp default_compression_level(runtime_tuning) do
    runtime_tuning.compression_level
  end

  defp recommended_compression_level(:conservative), do: 0
  defp recommended_compression_level(:cpu_limited_throughput), do: 0
  defp recommended_compression_level(:cpu_limited_balanced), do: 0
  defp recommended_compression_level(:balanced), do: 1
  defp recommended_compression_level(:throughput), do: 1

  defp recommended_num_parallel_collections(schedulers_online, nil, _tuning_profile) do
    schedulers_online
    |> div(2)
    |> max(1)
    |> min(4)
  end

  defp recommended_num_parallel_collections(
         schedulers_online,
         mem_available_bytes,
         tuning_profile
       ) do
    case tuning_profile do
      :conservative ->
        1

      :cpu_limited_throughput ->
        cond do
          mem_available_bytes < gib(2.5) -> 1
          true -> min(schedulers_online, 2)
        end

      :cpu_limited_balanced ->
        min(schedulers_online, 2)

      :balanced ->
        cond do
          mem_available_bytes < gib(6) -> min(schedulers_online, 2)
          mem_available_bytes < gib(12) -> min(schedulers_online, 4)
          true -> min(schedulers_online, 6)
        end

      :throughput ->
        cond do
          mem_available_bytes < gib(12) -> min(schedulers_online, 4)
          true -> min(schedulers_online, 8)
        end
    end
  end

  defp recommended_pigz_threads(schedulers_online, nil, _tuning_profile) do
    schedulers_online
    |> div(2)
    |> max(1)
    |> min(4)
  end

  defp recommended_pigz_threads(schedulers_online, mem_available_bytes, tuning_profile) do
    case tuning_profile do
      :conservative ->
        1

      :cpu_limited_throughput ->
        1

      :cpu_limited_balanced ->
        1

      :balanced ->
        cond do
          mem_available_bytes < gib(6) -> min(max(schedulers_online - 1, 1), 2)
          mem_available_bytes < gib(12) -> min(max(schedulers_online - 1, 1), 3)
          true -> min(max(schedulers_online - 1, 1), 4)
        end

      :throughput ->
        cond do
          mem_available_bytes < gib(12) -> min(max(schedulers_online - 1, 1), 4)
          true -> min(max(schedulers_online - 1, 1), 8)
        end
    end
  end

  defp recommended_s3_max_concurrent_requests(schedulers_online, nil, _tuning_profile) do
    schedulers_online
    |> Kernel.*(2)
    |> max(4)
    |> min(8)
  end

  defp recommended_s3_max_concurrent_requests(
         schedulers_online,
         mem_available_bytes,
         tuning_profile
       ) do
    case tuning_profile do
      :conservative ->
        4

      :cpu_limited_throughput ->
        cond do
          mem_available_bytes < gib(2.5) -> 4
          true -> min(max(schedulers_online * 2, 4), 8)
        end

      :cpu_limited_balanced ->
        min(max(schedulers_online * 2, 4), 6)

      :balanced ->
        cond do
          mem_available_bytes < gib(6) -> min(max(schedulers_online * 2, 4), 8)
          true -> min(max(schedulers_online * 3, 6), 12)
        end

      :throughput ->
        cond do
          mem_available_bytes < gib(12) -> min(max(schedulers_online * 3, 8), 16)
          true -> min(max(schedulers_online * 4, 12), 24)
        end
    end
  end

  defp recommended_s3_max_queue_size(schedulers_online, nil, _tuning_profile) do
    schedulers_online
    |> Kernel.*(128)
    |> max(256)
    |> min(512)
  end

  defp recommended_s3_max_queue_size(schedulers_online, mem_available_bytes, tuning_profile) do
    case tuning_profile do
      :conservative ->
        256

      :cpu_limited_throughput ->
        cond do
          mem_available_bytes < gib(2.5) -> 256
          true -> min(max(schedulers_online * 192, 256), 512)
        end

      :cpu_limited_balanced ->
        min(max(schedulers_online * 160, 256), 512)

      :balanced ->
        cond do
          mem_available_bytes < gib(6) -> min(max(schedulers_online * 192, 256), 768)
          true -> min(max(schedulers_online * 256, 512), 1024)
        end

      :throughput ->
        cond do
          mem_available_bytes < gib(12) -> min(max(schedulers_online * 256, 512), 1024)
          true -> min(max(schedulers_online * 384, 1024), 2048)
        end
    end
  end

  defp recommended_s3_multipart_chunksize_mib(_schedulers_online, nil, _tuning_profile), do: 32

  defp recommended_s3_multipart_chunksize_mib(
         _schedulers_online,
         mem_available_bytes,
         tuning_profile
       ) do
    case tuning_profile do
      :conservative ->
        16

      :cpu_limited_throughput ->
        cond do
          mem_available_bytes < gib(2.5) -> 16
          true -> 32
        end

      :cpu_limited_balanced ->
        32

      :balanced ->
        cond do
          mem_available_bytes < gib(6) -> 32
          true -> 64
        end

      :throughput ->
        cond do
          mem_available_bytes < gib(12) -> 64
          true -> 96
        end
    end
  end

  defp recommended_meter_block_size_mib(_schedulers_online, nil, _tuning_profile), do: 16

  defp recommended_meter_block_size_mib(_schedulers_online, mem_available_bytes, tuning_profile) do
    case tuning_profile do
      :conservative ->
        8

      :cpu_limited_throughput ->
        cond do
          mem_available_bytes < gib(2.5) -> 8
          true -> 16
        end

      :cpu_limited_balanced ->
        16

      :balanced ->
        cond do
          mem_available_bytes < gib(6) -> 16
          true -> 32
        end

      :throughput ->
        cond do
          mem_available_bytes < gib(12) -> 32
          true -> 64
        end
    end
  end

  defp gib(value) when is_integer(value), do: value * 1024 * 1024 * 1024
  defp gib(value) when is_float(value), do: trunc(value * 1024 * 1024 * 1024)
end

DocdbStreamBackup.main(System.argv())
