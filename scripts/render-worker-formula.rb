#!/usr/bin/env ruby
# frozen_string_literal: true

version, checksums = ARGV
unless ARGV.length == 2 && version.match?(/\A\d+\.\d+\.\d+\z/)
  abort "Usage: #{$PROGRAM_NAME} VERSION SHA256SUMS"
end

archive = "prime-worker-#{version}-source.tar.gz"
digests = File.foreach(checksums).each_with_object([]) do |line, matches|
  digest, name = line.split
  matches << digest if name == archive
end
unless digests.size == 1 && digests.first.match?(/\A[0-9a-f]{64}\z/)
  abort "Expected one valid SHA-256 checksum for #{archive}"
end

puts <<~FORMULA
  class PrimeWorker < Formula
    desc "Volunteer worker for the Test Math prime archive"
    homepage "https://test-math.notallow.cc/"
    url "https://github.com/VachaganGrigoryan/prime-finder/releases/download/worker-v#{version}/#{archive}"
    version "#{version}"
    sha256 "#{digests.first}"
    license "MIT"

    depends_on "cmake" => :build
    depends_on "pkgconf" => :build
    depends_on "rust" => :build
    depends_on :macos

    deny_network_access!

    def fetch
      system "cargo", "fetch", *std_cargo_fetch_args
    end

    def install
      system "cargo", "install", *std_cargo_args(features: "volunteer-worker"),
             "--no-default-features", "--bin", "prime-worker"
      pkgshare.install "LICENSE"
    end

    test do
      assert_match "https://test-math-workers.notallow.cc/join", shell_output("\#{bin}/prime-worker join")
    end
  end
FORMULA
