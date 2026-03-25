#!/usr/bin/env elixir


defmodule DocdbStreamBackup do
  @default_prefix "docdb/"
  @default_expected_size_bytes 10 * 1024 * 1024 * 1024

  @usage """
  Uso:
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket>
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> <prefix>
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> [--prefix docdb/prod] [--num-parallel-collections 16] [--pigz-threads 8] [--compression-level 1] [--expected-size-bytes 10737418240]

  Exemplos:
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false' meu-bucket
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket docdb/prod
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket --num-parallel-collections 16 --pigz-threads 8 --compression-level 1 --expected-size-bytes 10737418240

  Observação:
    O upload acontece por stream em memória, sem gerar arquivo local no EC2.
    Perfil padrão otimizado para throughput: compressão nível 1 e expected-size de 10 GiB.
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
             :ok <- run_pipeline(args, key) do
          IO.puts("backup concluído")
          IO.puts("destino: s3://#{args.bucket}/#{key}")
          :ok
        else
          {:error, message} ->
            IO.puts(:stderr, "erro: #{message}")
            IO.puts(:stderr, @usage)
            System.halt(1)
        end

      {:error, message} ->
        IO.puts(:stderr, "erro: #{message}")
        IO.puts(:stderr, @usage)
        System.halt(1)
    end
  end

  defp parse_args(argv) do
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
             {:ok, extra_mongodump_args} <- resolve_mongodump_args(options),
             args <- %{
               uri: normalized_uri,
               bucket: normalized_bucket,
               prefix: normalized_prefix,
               num_parallel_collections: num_parallel_collections,
               pigz_threads: pigz_threads,
               compression_level: compression_level,
               expected_size_bytes: expected_size_bytes,
               extra_mongodump_args: extra_mongodump_args
             } do
          {:ok, args}
        end
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

    {:ok, extra_args}
  end

  defp default_num_parallel_collections do
    System.schedulers_online()
    |> Kernel.*(2)
    |> max(8)
    |> min(32)
  end

  defp default_pigz_threads do
    System.schedulers_online()
    |> max(1)
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
    destination = "s3://#{args.bucket}/#{key}"

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

    command =
      [mongodump_command, pigz_command, aws_command]
      |> Enum.join(" | ")

    IO.puts(
      "config: numParallelCollections=#{args.num_parallel_collections} pigz_threads=#{args.pigz_threads} compression_level=#{args.compression_level} expected_size_bytes=#{args.expected_size_bytes}"
    )

    case System.cmd("bash", ["-o", "pipefail", "-c", command],
           into: IO.binstream(:stdio, :line),
           stderr_to_stdout: true
         ) do
      {_, 0} -> :ok
      {_, status} -> {:error, "pipeline falhou com código #{status}"}
    end
  end

  defp shell_escape(value) do
    escaped = String.replace(value, "'", "'\\''")
    "'#{escaped}'"
  end
end

DocdbStreamBackup.main(System.argv())
