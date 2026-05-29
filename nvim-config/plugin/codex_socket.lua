local uv = vim.uv or vim.loop

local function safe_unlink(path)
  pcall(os.remove, path)
end

local function link_stable_socket(stable_path, target_path)
  if not target_path or target_path == "" then
    return
  end

  safe_unlink(stable_path)
  pcall(vim.loop.fs_symlink, target_path, stable_path)
end

local function ensure_server_socket()
  local stable_socket = vim.fn.stdpath("state") .. "/codex-lazyvim.sock"

  if vim.v.servername == nil or vim.v.servername == "" then
    local started = pcall(vim.fn.serverstart, stable_socket)
    if started then
      return
    end

    local stat = uv and uv.fs_stat and uv.fs_stat(stable_socket) or nil
    if stat and stat.type == "socket" then
      safe_unlink(stable_socket)
      pcall(vim.fn.serverstart, stable_socket)
      return
    end
  end

  -- Neovim may auto-start a dynamic socket; keep a stable alias for automation.
  link_stable_socket(stable_socket, vim.v.servername)
end

vim.api.nvim_create_autocmd("VimEnter", {
  callback = ensure_server_socket,
  once = true,
})
