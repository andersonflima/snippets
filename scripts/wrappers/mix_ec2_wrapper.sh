#!/usr/bin/env elixir

defmodule MixEc2Wrapper do
  @default_remote_commands [
    "deps.get",
    "deps.compile",
    "deps.update",
    "deps.unlock",
    "local.hex",
    "local.rebar",
    "archive.install",
    "archive.build",
    "phx.new",
    "hex.info"
  ]

  def main(args) do
    if route_to_remote?(args) do
      run_remote!(args)
    else
      Mix.CLI.main(args)
    end
  end

  defp route_to_remote?([]), do: false

  defp route_to_remote?([first_arg | _rest]) do
    cond do
      truthy_env?("MIX_WRAPPER_DISABLE_REMOTE") ->
        false

      truthy_env?("MIX_WRAPPER_FORCE_REMOTE") ->
        true

      first_arg in ["-h", "--help", "help", "--version", "-v"] ->
        false

      true ->
        first_arg in remote_commands()
    end
  end

  defp remote_commands do
    case System.get_env("MIX_WRAPPER_REMOTE_COMMANDS") do
      nil ->
        @default_remote_commands

      csv ->
        csv
        |> String.split(",", trim: true)
        |> Enum.reject(&(&1 == ""))
    end
  end

  defp truthy_env?(name) do
    case System.get_env(name) do
      nil -> false
      value -> String.downcase(value) in ["1", "true", "yes", "on"]
    end
  end

  defp run_remote!(args) do
    entrypoint = remote_entrypoint()

    File.regular?(entrypoint) || abort("entrypoint mix-via-ec2 não encontrado: #{entrypoint}")
    File.stat!(entrypoint).access in [:read, :read_write] || abort("entrypoint mix-via-ec2 não está acessível: #{entrypoint}")

    IO.puts(:stderr, "[mix-ec2-wrapper] delegando para o EC2: mix #{Enum.join(args, " ")}")

    {_, status} =
      System.cmd(entrypoint, ["--" | args],
        into: IO.stream(:stdio, :line),
        stderr_to_stdout: true
      )

    System.halt(status)
  end

  defp remote_entrypoint do
    case System.get_env("MIX_VIA_EC2_ENTRYPOINT") do
      nil ->
        System.argv()
        |> current_script_dir()
        |> Path.join("mix-via-ec2")

      path ->
        path
    end
  end

  defp current_script_dir(_argv) do
    __ENV__.file
    |> Path.expand()
    |> Path.dirname()
  end

  defp abort(message) do
    IO.puts(:stderr, "[mix-ec2-wrapper] erro: #{message}")
    System.halt(1)
  end
end

MixEc2Wrapper.main(System.argv())
