defmodule Mongo.Mixfile do
  use Mix.Project

  def project do
    [ app: :mongo,
      name: "mongo",
      version: "0.5.5",
      elixir: "> 1.3.0",
      source_url: "https://github.com/ejoy/elixir-mongo",
      description: "MongoDB driver for Elixir",
      deps: deps(),
      package: package(),
      docs: &docs/0 ]
  end

  # Configuration for the OTP application
  def application do
    [
      applications: [:logger],
      env: [host: {"127.0.0.1", 27017}]
    ]
  end

  # Returns the list of dependencies for prod
  defp deps() do
    [
      {:ex_doc, ">= 0.0.0", only: :doc },
      {:earmark, ">= 0.0.0", only: :doc},
      {:cbson, github: "sean-lin/elixir-cbson"},
    ]
  end

  defp docs do
    [ #readme: false,
      #main: "README",
      source_ref: System.cmd("git", ["rev-parse", "--verify", "--quiet", "HEAD"])|>elem(0) ]
  end

  defp package do
    [ contributors: ["jerp"],
      licenses: ["MIT"],
      links: %{
        "GitHub" => "https://github.com/ejoy/elixir-mongo",
        "Documentation" => "https://checkiz.github.io/elixir-mongo"
      } ]
  end

end
