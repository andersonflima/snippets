#!/usr/bin/env elixir


defmodule DocdbStreamBackup do
  @default_prefix "docdb/"

  @usage """
  Uso:
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket>
    elixir scripts/docdb_stream_backup.exs <docdb_uri> <bucket> <prefix>

  Exemplos:
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false' meu-bucket
    elixir scripts/docdb_stream_backup.exs 'mongodb://user:pass@host:27017/?tls=true&replicaSet=rs0' meu-bucket docdb/prod

  Observação:
    O upload acontece por stream em memória, sem gerar arquivo local no EC2.
  """

  def main(argv) do
    with {:ok, args} <- parse_args(argv),
         :ok <- ensure_binary("bash"),
         :ok <- ensure_binary("mongodump"),
         :ok <- ensure_binary("pigz"),
         :ok <- ensure_binary("aws"),
         {:ok, key} <- build_s3_key(args.prefix),
         :ok <- run_pipeline(args.uri, args.bucket, key) do
      IO.puts("backup concluído")
      IO.puts("destino: s3://#{args.bucket}/#{key}")
      :ok
    else
      {:error, message} ->
        IO.puts(:stderr, "erro: #{message}")
        IO.puts(:stderr, @usage)
        System.halt(1)
    end
  end

  defp parse_args([uri, bucket]) do
    parse_args([uri, bucket, @default_prefix])
  end

  defp parse_args([uri, bucket, prefix]) do
    with {:ok, normalized_uri} <- normalize_non_empty(uri, "docdb_uri"),
         {:ok, normalized_bucket} <- normalize_non_empty(bucket, "bucket"),
         {:ok, normalized_prefix} <- normalize_prefix(prefix) do
      {:ok, %{uri: normalized_uri, bucket: normalized_bucket, prefix: normalized_prefix}}
    end
  end

  defp parse_args(_), do: {:error, "argumentos inválidos"}

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

  defp run_pipeline(uri, bucket, key) do
    destination = "s3://#{bucket}/#{key}"

    command =
      [
        "mongodump --uri",
        shell_escape(uri),
        "--archive",
        "|",
        "pigz -c",
        "|",
        "aws s3 cp -",
        shell_escape(destination)
      ]
      |> Enum.join(" ")

    case System.cmd("bash", ["-o", "pipefail", "-c", command], into: IO.binstream(:stdio, :line), stderr_to_stdout: true) do
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
