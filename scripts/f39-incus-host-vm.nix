{ pkgs, ... }:
# A throwaway NixOS VM that IS the Incus host for the lnrent-spec F39 run.
#
# Why a VM and not this box: the channel's incus module asserts
#   "Incus on NixOS is unsupported using iptables. Set networking.nftables.enable = true"
# and nftables in turn asserts networking.firewall.extraCommands == "". The host's
# extraCommands is its IPv6 WAN guard, so Incus cannot go on the host without
# rewriting a load-bearing firewall. In here, nftables is free.
#
# L2 nesting (Incus runs its own VM inside this one) works because the host has
# kvm_amd nested=1 and we pass -cpu host.
{
  networking.nftables.enable = true;
  networking.hostName = "f39host";
  virtualisation.incus.enable = true;

  users.users.f39 = {
    isNormalUser = true;
    extraGroups = [ "wheel" "incus-admin" ];
    initialPassword = "f39";
  };
  security.sudo.wheelNeedsPassword = false;

  services.openssh = {
    enable = true;
    settings.PermitRootLogin = "prohibit-password";
  };
  # Throwaway key generated beside this file, so the run can drive the VM
  # non-interactively. The VM is disposable and reachable only on loopback.
  users.users.root.openssh.authorizedKeys.keys = [
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIIi8QtkmU1HSRALVrqDsN6hA9Rcu+h6pNxWdGnSdlpaG master@nixos"
  ];
  users.users.f39.openssh.authorizedKeys.keys = [
    "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIIi8QtkmU1HSRALVrqDsN6hA9Rcu+h6pNxWdGnSdlpaG master@nixos"
  ];

  # Everything the wizard shells out to.
  environment.systemPackages = with pkgs; [
    curl openssh coreutils gnutar xz gawk gnugrep python3 iproute2
  ];

  system.stateVersion = "24.11";

  virtualisation.vmVariant.virtualisation = {
    memorySize = 8192;
    cores = 6;
    diskSize = 24576;
    graphics = false;
    # -cpu host exposes SVM so Incus can start a VM inside this VM.
    qemu.options = [ "-cpu host" ];
    # No forwardPorts: another qemu on this box already holds 2222. The ssh
    # forward is passed at runtime via QEMU_NET_OPTS with a port chosen free.
  };
}
