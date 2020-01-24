class S5cmd < Formula
  desc "Parallel S3 and local filesystem execution tool"
  homepage "https://github.com/peak/s5cmd"

  # Source code archive. Each tagged release will have one
  url "https://github.com/peak/s5cmd/archive/v0.6.2.tar.gz"
  sha256 "c38cbe80465ff0ccde95a2bbdbcd2bcd8f0326d074d7f8e4a993d586dca28ef8"

  depends_on "go" => :build

  def install
    ENV["GOPATH"] = buildpath

    bin_path = buildpath/"src/github.com/peak/s5cmd"
    # Copy all files from their current location (GOPATH root)
    # to $GOPATH/src/github.com/peak/s3hash
    bin_path.install Dir["*"]
    cd bin_path do
      # Install the compiled binary into Homebrew's `bin` - a pre-exisxting
      # global variable
      system "go", "build", "-o", bin/"s5cmd"
    end

    ohai "To install shell completion, run s5cmd -cmp-install"
  end

  test do
    assert_match "s5cmd version v0.6.2", shell_output("#{bin}/s5cmd -version 2>&1", 0)
  end
end
