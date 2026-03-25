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
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> --parallel-databases [--database app] [--database analytics] [--database-concurrency 2]
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> --mongodump-arg --tls --mongodump-arg --tlsCAFile=/path/ca.pem
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> --mongodump-arg='--tls' --mongodump-arg='--tlsCAFile=/path/ca.pem'

  Exemplos:
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false' meu-bucket
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket docdb/prod
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket --num-parallel-collections 16 --pigz-threads 8 --compression-level 1 --expected-size-bytes 10737418240
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket --parallel-databases --database app --database analytics --database-concurrency 2

  Observação:
    O upload acontece por stream em memória, sem gerar arquivo local no EC2.
    Os defaults de paralelismo são ajustados automaticamente por RAM/CPU do host para reduzir risco de OOM no mongodump.
    O perfil padrão mantém compressão nível 1 e expected-size de 10 GiB.
    No modo --parallel-databases, cada database gera um objeto separado sob o prefixo docdb-backup-<timestamp>/.
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
        {:error, "opções inválidas: #{Enum.map_join(invalid_options, ", ", &format_invalid_option/1)}"}

      true ->
        with {:ok, positional} <- parse_positional_args(positional_args),
             {:ok, normalized_uri} <- normalize_non_empty(positional.uri, "docdb_uri"),
             {:ok, validated_uri} <- validate_docdb_uri(normalized_uri),
             {:ok, normalized_bucket} <- normalize_non_empty(positional.bucket, "bucket"),
             {:ok, normalized_prefix} <- resolve_prefix(positional.prefix, options[:prefix]),
             {:ok, database_names} <- resolve_database_names(options),
             {:ok, parallel_databases} <- resolve_parallel_databases(options, database_names),
             {:ok, database_concurrency} <-
               resolve_database_concurrency(options[:database_concurrency], runtime_tuning, parallel_databases),
             {:ok, num_parallel_collections} <-
               resolve_positive_integer(
                 options[:num_parallel_collections],
                 runtime_tuning.num_parallel_collections,
                 "num_parallel_collections"
               ),
             {:ok, pigz_threads} <-
               resolve_positive_integer(options[:pigz_threads], runtime_tuning.pigz_threads, "pigz_threads"),
             {:ok, compression_level} <- resolve_compression_level(options[:compression_level]),
             {:ok, expected_size_bytes} <- resolve_expected_size_bytes(options),
             {:ok, extra_mongodump_args} <- resolve_mongodump_args(options),
             :ok <- validate_parallel_database_args(parallel_databases, database_names, extra_mongodump_args) do
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
             expected_size_bytes: expected_size_bytes,
             extra_mongodump_args: extra_mongodump_args,
             runtime_tuning: runtime_tuning,
             num_parallel_collections_source: option_source(options[:num_parallel_collections]),
             pigz_threads_source: option_source(options[:pigz_threads])
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
    {:ok, options[:parallel_databases] == true or database_names != [] or not is_nil(options[:database_concurrency])}
  end

  defp resolve_database_concurrency(_database_concurrency, _runtime_tuning, false), do: {:ok, 1}

  defp resolve_database_concurrency(nil, runtime_tuning, true) do
    {:ok, default_database_concurrency(runtime_tuning)}
  end

  defp resolve_database_concurrency(database_concurrency, _runtime_tuning, true) do
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
    if args.parallel_databases do
      run_parallel_database_pipelines(args, capabilities)
    else
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

  defp run_parallel_database_pipelines(args, capabilities) do
    started_at = System.monotonic_time(:microsecond)

    with {:ok, target_databases} <- resolve_target_databases(args),
         {:ok, session_prefix} <- build_parallel_session_prefix(args.prefix) do
      parallel_args = apply_parallel_database_runtime_tuning(args, target_databases)
      per_database_expected_size_bytes = expected_size_bytes_per_database(args.expected_size_bytes, length(target_databases))

      print_parallel_database_config(parallel_args, capabilities, target_databases, session_prefix, per_database_expected_size_bytes)

      stage_specs =
        target_databases
        |> Enum.map(fn database_name ->
          %{name: database_name, activity_label: "stream", parallelism: 1}
        end)

      progress_display = start_progress_display("backup paralelo por database", stage_specs)
      initial_stage_states = Map.new(target_databases, &{&1, {:running, nil}})

      result =
        target_databases
        |> Task.async_stream(
          fn database_name ->
            run_parallel_database_pipeline(
              parallel_args,
              capabilities,
              session_prefix,
              database_name,
              per_database_expected_size_bytes
            )
          end,
          max_concurrency: parallel_args.database_concurrency,
          ordered: false,
          timeout: :infinity
        )
        |> Enum.reduce(
          %{stage_states: initial_stage_states, destinations: [], errors: []},
          fn task_result, acc ->
            reduce_parallel_database_result(task_result, acc, progress_display)
          end
        )

      stop_progress_display(progress_display, result.stage_states)

      case result.errors do
        [] ->
          {:ok,
           %{
             mode: :parallel_databases,
             metrics: %{
               duration_us: System.monotonic_time(:microsecond) - started_at,
               raw_bytes: 0,
               estimated_bytes: args.expected_size_bytes
             },
             destinations: Enum.sort(result.destinations)
           }}

        errors ->
          {:error,
           format_parallel_database_errors(errors),
           %{
             duration_us: System.monotonic_time(:microsecond) - started_at,
             raw_bytes: 0,
             estimated_bytes: 0
           }}
      end
    end
  end

  defp resolve_target_databases(%{database_names: []} = args) do
    discover_databases(args.uri)
  end

  defp resolve_target_databases(%{database_names: database_names}) do
    {:ok, database_names}
  end

  defp discover_databases(uri) do
    with {:ok, discovery_command} <- database_discovery_command(uri),
         {output, 0} <- run_database_discovery(discovery_command),
         {:ok, database_names} <- parse_discovered_databases(output) do
      case filter_discovered_databases(database_names) do
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

  defp database_discovery_command(uri) do
    script = "db.adminCommand({ listDatabases: 1, nameOnly: true }).databases.forEach((item) => console.log(item.name))"

    cond do
      System.find_executable("mongosh") ->
        {:ok, {"mongosh", ["--quiet", uri, "--eval", script]}}

      System.find_executable("mongo") ->
        legacy_script = "db.adminCommand({ listDatabases: 1, nameOnly: true }).databases.forEach(function(item) { print(item.name) })"
        {:ok, {"mongo", ["--quiet", uri, "--eval", legacy_script]}}

      true ->
        {:error,
         "modo --parallel-databases sem --database exige mongosh ou mongo no PATH para descobrir os databases"}
    end
  end

  defp run_database_discovery({binary, command_args}) do
    System.cmd(binary, command_args, stderr_to_stdout: true)
  rescue
    error ->
      {:error, Exception.message(error)}
  end

  defp parse_discovered_databases(output) do
    database_names =
      output
      |> String.split("\n", trim: true)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == "" or &1 == "undefined"))
      |> Enum.uniq()

    {:ok, database_names}
  end

  defp filter_discovered_databases(database_names) do
    database_names
    |> Enum.reject(&(&1 in ["admin", "config", "local"]))
  end

  defp apply_parallel_database_runtime_tuning(args, target_databases) do
    effective_database_concurrency = effective_parallel_database_concurrency(args, target_databases)

    %{
      args
      | database_concurrency: effective_database_concurrency,
        num_parallel_collections: parallel_database_num_parallel_collections(args),
        pigz_threads: parallel_database_pigz_threads(args)
    }
    |> distribute_parallel_database_runtime_budgets()
  end

  defp effective_parallel_database_concurrency(args, target_databases) do
    target_databases
    |> length()
    |> min(max(args.database_concurrency, 1))
    |> max(1)
  end

  defp distribute_parallel_database_runtime_budgets(args) do
    %{
      args
      | num_parallel_collections:
          distribute_parallel_budget(args.num_parallel_collections, args.database_concurrency, args.num_parallel_collections_source),
        pigz_threads:
          distribute_parallel_budget(args.pigz_threads, args.database_concurrency, args.pigz_threads_source)
    }
  end

  defp distribute_parallel_budget(total_budget, _database_concurrency, :cli), do: total_budget

  defp distribute_parallel_budget(total_budget, database_concurrency, :auto) do
    total_budget
    |> div(max(database_concurrency, 1))
    |> max(1)
  end

  defp parallel_database_num_parallel_collections(%{num_parallel_collections_source: :auto} = args),
    do: args.num_parallel_collections

  defp parallel_database_num_parallel_collections(args), do: args.num_parallel_collections

  defp parallel_database_pigz_threads(%{pigz_threads_source: :auto} = args), do: args.pigz_threads
  defp parallel_database_pigz_threads(args), do: args.pigz_threads

  defp expected_size_bytes_per_database(expected_size_bytes, database_count) do
    expected_size_bytes
    |> div(max(database_count, 1))
    |> max(1_048_576)
  end

  defp print_parallel_database_config(args, capabilities, target_databases, session_prefix, per_database_expected_size_bytes) do
    print_config(args, capabilities)
    IO.puts(
      "modo: parallel_databases database_concurrency=#{args.database_concurrency} databases=#{length(target_databases)} per_pipeline=#{format_parallel_runtime(args, capabilities)}"
    )
    IO.puts("destino-base: s3://#{args.bucket}/#{session_prefix}")
    IO.puts("expected_size_por_database: #{format_bytes_binary(per_database_expected_size_bytes)}")
    IO.puts("databases: #{Enum.join(target_databases, ", ")}")
  end

  defp run_parallel_database_pipeline(args, capabilities, session_prefix, database_name, per_database_expected_size_bytes) do
    key = build_parallel_database_key(session_prefix, database_name)

    database_args = %{
      args
      | expected_size_bytes: per_database_expected_size_bytes,
        extra_mongodump_args: args.extra_mongodump_args ++ ["--db=#{database_name}"]
    }

    case run_pipeline(database_args, capabilities, key,
           show_config?: false,
           show_progress?: false,
           print_output?: false
         ) do
      {:ok, metrics} ->
        {:ok, %{database_name: database_name, destination: "s3://#{args.bucket}/#{key}", metrics: metrics}}

      {:error, message, metrics} ->
        {:error, %{database_name: database_name, message: message, metrics: metrics}}
    end
  end

  defp build_parallel_database_key(session_prefix, database_name) do
    "#{session_prefix}#{sanitize_s3_segment(database_name)}.archive.gz"
  end

  defp sanitize_s3_segment(segment) do
    segment
    |> String.trim()
    |> String.replace(~r{[^a-zA-Z0-9._-]+}, "_")
  end

  defp reduce_parallel_database_result({:ok, {:ok, result}}, acc, progress_display) do
    update_progress_display(progress_display, result.database_name, {:done, 0})

    %{
      acc
      | stage_states: Map.put(acc.stage_states, result.database_name, {:done, 0}),
        destinations: [result.destination | acc.destinations]
    }
  end

  defp reduce_parallel_database_result({:ok, {:error, error}}, acc, progress_display) do
    update_progress_display(progress_display, error.database_name, {:failed, 1})

    %{
      acc
      | stage_states: Map.put(acc.stage_states, error.database_name, {:failed, 1}),
        errors: [error | acc.errors]
    }
  end

  defp reduce_parallel_database_result({:exit, reason}, acc, _progress_display) do
    error = %{database_name: "desconhecido", message: "task abortada: #{inspect(reason)}", metrics: %{}}
    %{acc | errors: [error | acc.errors]}
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

    show_config? = Keyword.get(opts, :show_config?, true)
    show_progress? = Keyword.get(opts, :show_progress?, true)
    print_output? = Keyword.get(opts, :print_output?, true)

    if show_config? do
      print_config(args, capabilities)
      IO.puts("destino: #{destination}")
      IO.puts("alvo: #{format_bytes_binary(@default_expected_size_bytes)} em até #{@default_target_duration_seconds}s")
    end

    progress_display =
      if show_progress? do
        start_progress_display("backup em andamento", progress_stage_specs(args, capabilities))
      else
        nil
      end

    case System.cmd("bash", ["-c", command], stderr_to_stdout: true) do
      {output, 0} ->
        maybe_stop_progress_display(progress_display, success_stage_states())

        if print_output? do
          print_pipeline_output(output, status_probe, stderr_markers)
        end

        {:ok,
         %{
           duration_us: System.monotonic_time(:microsecond) - started_at,
           raw_bytes: 0,
           estimated_bytes: args.expected_size_bytes
         }}

      {output, status} ->
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
      "config: numParallelCollections=#{num_parallel_display} (#{Map.get(args, :num_parallel_collections_source, :auto)}) pigz_threads=#{args.pigz_threads} (#{Map.get(args, :pigz_threads_source, :auto)}) compression_level=#{args.compression_level} expected_size=#{format_bytes_binary(args.expected_size_bytes)}"
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

    "numParallelCollections=#{num_parallel_display} pigz_threads=#{args.pigz_threads} compression_level=#{args.compression_level}"
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
      %{name: "aws", activity_label: "streams", parallelism: 1}
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

      {:update, stage_name, stage_state} ->
        state
        |> put_in([:stage_states, stage_name], stage_state)
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
  defp maybe_stop_progress_display(pid, stage_states), do: stop_progress_display(pid, stage_states)

  defp update_progress_display(nil, _stage_name, _stage_state), do: :ok

  defp update_progress_display(pid, stage_name, stage_state) do
    send(pid, {:update, stage_name, stage_state})
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
      | Enum.map(state.stage_specs, fn stage_spec ->
          format_progress_stage_line(stage_spec, Map.get(state.stage_states, stage_spec.name, {:running, nil}), state.frame)
        end)
    ]
  end

  defp format_progress_stage_line(stage_spec, {:running, _status} = stage_state, frame) do
    "#{String.pad_trailing(stage_spec.name, 10)} #{indeterminate_bar(frame, 20)} running | #{format_stage_parallelism(stage_spec, stage_state, frame)}"
  end

  defp format_progress_stage_line(stage_spec, {:done, status} = stage_state, frame) do
    "#{String.pad_trailing(stage_spec.name, 10)} #{String.duplicate("#", 20)} done (#{status}) | #{format_stage_parallelism(stage_spec, stage_state, frame)}"
  end

  defp format_progress_stage_line(stage_spec, {:failed, status} = stage_state, frame) do
    "#{String.pad_trailing(stage_spec.name, 10)} #{String.duplicate("!", 20)} failed (#{status}) | #{format_stage_parallelism(stage_spec, stage_state, frame)}"
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

  defp format_parallelism_slots(slot_count, {:done, _status}, _frame) do
    String.duplicate("#", slot_count)
  end

  defp format_parallelism_slots(slot_count, {:failed, _status}, _frame) do
    String.duplicate("!", slot_count)
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

  defp default_runtime_tuning do
    schedulers_online = System.schedulers_online()
    mem_available_bytes = read_mem_available_bytes()
    tuning_profile = infer_tuning_profile(schedulers_online, mem_available_bytes)

    %{
      schedulers_online: schedulers_online,
      mem_available_bytes: mem_available_bytes,
      tuning_profile: tuning_profile,
      num_parallel_collections:
        recommended_num_parallel_collections(schedulers_online, mem_available_bytes, tuning_profile),
      pigz_threads: recommended_pigz_threads(schedulers_online, mem_available_bytes, tuning_profile)
    }
  end

  defp read_mem_available_bytes do
    with {:ok, meminfo} <- File.read("/proc/meminfo"),
         [value_kib] <- Regex.run(~r/^MemAvailable:\s+(\d+)\s+kB$/m, meminfo, capture: :all_but_first),
         {parsed_kib, ""} <- Integer.parse(value_kib) do
      parsed_kib * 1024
    else
      _ -> nil
    end
  end

  defp infer_tuning_profile(schedulers_online, nil) when schedulers_online <= 2, do: :cpu_limited_balanced
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

  defp recommended_num_parallel_collections(schedulers_online, nil, _tuning_profile) do
    schedulers_online
    |> div(2)
    |> max(1)
    |> min(4)
  end

  defp recommended_num_parallel_collections(schedulers_online, mem_available_bytes, tuning_profile) do
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

  defp gib(value) when is_integer(value), do: value * 1024 * 1024 * 1024
  defp gib(value) when is_float(value), do: trunc(value * 1024 * 1024 * 1024)
end

DocdbStreamBackup.main(System.argv())
