#!/usr/bin/env ruby
# frozen_string_literal: true

version, checksums = ARGV
abort "Usage: #{$PROGRAM_NAME} VERSION SHA256SUMS" unless version&.match?(/\A\d+\.\d+\.\d+\z/) && checksums

digests = {}
File.foreach(checksums) do |line|
  digest, name = line.split
  next unless name&.match?(/\Aprime-worker-(aarch64|x86_64)-apple-darwin\.tar\.gz\z/)

  abort "Invalid or duplicate checksum for #{name}" if !digest.match?(/\A[0-9a-f]{64}\z/) || digests.key?(name)

  digests[name] = digest
end

arm = digests["prime-worker-aarch64-apple-darwin.tar.gz"] or abort "Missing Apple Silicon checksum"
intel = digests["prime-worker-x86_64-apple-darwin.tar.gz"] or abort "Missing Intel checksum"

puts <<~CASK
  cask "prime-worker" do
    arch arm: "aarch64", intel: "x86_64"

    version "#{version}"
    sha256 arm:   "#{arm}",
           intel: "#{intel}"

    url "https://github.com/VachaganGrigoryan/prime-finder/releases/download/worker-v\#{version}/prime-worker-\#{arch}-apple-darwin.tar.gz"
    name "Prime Worker"
    desc "Volunteer worker for the Test Math prime archive"
    homepage "https://test-math.notallow.cc/"

    depends_on :macos
    binary "prime-worker"

    caveats "Run prime-worker join to enroll, then prime-worker run to start contributing."
  end
CASK
