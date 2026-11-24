#!/usr/bin/env ruby
# frozen_string_literal: true

version, checksums, preview_tag = ARGV
unless (2..3).cover?(ARGV.length) && version.match?(/\A\d+\.\d+\.\d+\z/)
  abort "Usage: #{$PROGRAM_NAME} VERSION SHA256SUMS [PREVIEW_TAG]"
end

archive = "prime-worker-#{version}-source.tar.gz"
match = preview_tag&.match(/\Aworker-preview-v#{Regexp.escape(version)}(?:-r([1-9]\d*))?\z/)
abort "Preview tag must match the source version" if preview_tag && !match
preview = !preview_tag.nil?
formula = preview ? "PrimeWorkerPreview" : "PrimeWorker"
tag = preview_tag || "worker-v#{version}"
revision = match && match[1] ? "\n  revision #{match[1]}" : ""
conflict = preview ? "\n  conflicts_with \"vachagangrigoryan/tap/prime-worker\", because: \"both install the prime-worker executable\"" : ""
digests = File.foreach(checksums).each_with_object([]) do |line, matches|
  digest, name = line.split
  matches << digest if name == archive
end
unless digests.size == 1 && digests.first.match?(/\A[0-9a-f]{64}\z/)
  abort "Expected one valid SHA-256 checksum for #{archive}"
end

puts <<~FORMULA
  class #{formula} < Formula
    desc "Volunteer worker for the Test Math prime archive"
    homepage "https://test-math.notallow.cc/"
    url "https://github.com/VachaganGrigoryan/prime-finder/releases/download/#{tag}/#{archive}"
    version "#{version}"#{revision}
    sha256 "#{digests.first}"
    license "MIT"

    depends_on "cmake" => :build
    depends_on "pkgconf" => :build
    depends_on "rust" => :build
    depends_on :macos#{conflict}

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
