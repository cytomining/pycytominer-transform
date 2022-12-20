// project cuefile for Dagger CI and other development tooling related to this project.
package main

import "dagger.io/dagger"

import "universe.dagger.io/bash"

import "universe.dagger.io/docker"

// python build for linting, testing, building, etc.
#PythonBuild: {
	// client filesystem
	filesystem: dagger.#FS

	// python version to use for build
	python_ver: string | *"3.9"

	// poetry version to use for build
	poetry_ver: string | *"1.2"

	// container image
	output: _python_build.output

	// referential build for base python image
	_python_pre_build: docker.#Build & {
		steps: [
			docker.#Pull & {
				source: "python:" + python_ver
			},
			docker.#Run & {
				command: {
					name: "mkdir"
					args: ["/workdir"]
				}
			},
			docker.#Set & {
				config: {
					workdir: "/workdir"
					env: {
						POETRY_VIRTUALENVS_CREATE: "false"
					}
				}
			},
			docker.#Copy & {
				contents: filesystem
				source:   "./pyproject.toml"
				dest:     "/workdir/pyproject.toml"
			},
			docker.#Copy & {
				contents: filesystem
				source:   "./poetry.lock"
				dest:     "/workdir/poetry.lock"
			},
			docker.#Copy & {
				contents: filesystem
				source:   "./.pre-commit-config.yaml"
				dest:     "/workdir/.pre-commit-config.yaml"
			},
			docker.#Run & {
				command: {
					name: "pip"
					args: ["install", "--no-cache-dir", "poetry==" + poetry_ver]
				}
			},
			docker.#Run & {
				command: {
					name: "poetry"
					args: ["install", "--no-root", "--no-interaction", "--no-ansi"]
				}
			},
			// init for pre-commit install
			docker.#Run & {
				command: {
					name: "git"
					args: ["init"]
				}
			},
			docker.#Run & {
				command: {
					name: "poetry"
					args: ["run", "pre-commit", "install-hooks"]
				}
			},
		]
	}
	// python build with likely changes
	_python_build: docker.#Build & {
		steps: [
			docker.#Copy & {
				input:    _python_pre_build.output
				contents: filesystem
				source:   "./"
				dest:     "/workdir"
			},
		]
	}
}

// Convenience cuelang build for formatting, etc.
#CueBuild: {
	// client filesystem
	filesystem: dagger.#FS

	// output from the build
	output: _cue_build.output

	// cuelang pre-build
	_cue_pre_build: docker.#Build & {
		steps: [
			docker.#Pull & {
				source: "golang:latest"
			},
			docker.#Run & {
				command: {
					name: "mkdir"
					args: ["/workdir"]
				}
			},
			docker.#Run & {
				command: {
					name: "go"
					args: ["install", "cuelang.org/go/cmd/cue@latest"]
				}
			},
		]
	}
	// cue build for actions in this plan
	_cue_build: docker.#Build & {
		steps: [
			docker.#Copy & {
				input:    _cue_pre_build.output
				contents: filesystem
				source:   "./project.cue"
				dest:     "/workdir/project.cue"
			},
		]
	}

}

dagger.#Plan & {

	client: {
		filesystem: {
			"./": read: contents:             dagger.#FS
			"./project.cue": write: contents: actions.clean.cue.export.files."/workdir/project.cue"
			"./docs/build": write: contents:  actions.docs.sphinx.export.directories."/workdir/docs/build"
			"./htmlcov": write: contents:     actions.coverage.coverage.export.directories."/workdir/htmlcov"

			"./tests/data/cellprofiler": write: contents: actions.gather_data.cellprofiler.export.export.directories."/usr/local/src/output"
		}
	}
	python_version: string | *"3.9"
	poetry_version: string | *"1.2"

	actions: {

		// an internal python build for use with other actions
		_python_build: #PythonBuild & {
			filesystem: client.filesystem."./".read.contents
			python_ver: python_version
			poetry_ver: poetry_version
		}

		// an internal cue build for formatting/cleanliness
		_cue_build: #CueBuild & {
			filesystem: client.filesystem."./".read.contents
		}

		// gather data related to testing
		gather_data: {
			// gather cellprofiler related data
			cellprofiler: {
				build: docker.#Build & {
					steps: [
						docker.#Pull & {
							source: "cellprofiler/cellprofiler"
						},
						// gets, unzips, and exports results of example cellprofiler data
						bash.#Run & {
							script: contents: """
								# get example from https://cellprofiler.org/examples
								wget https://cellprofiler-examples.s3.amazonaws.com/ExampleHuman.zip -O ExampleHuman.zip

								# unzip
								jar xvf ExampleHuman.zip

								# make output dirs
								mkdir -p output/ExampleHuman

								# run cellprofiler against example pipeline
								# commands reference:
								# https://github.com/CellProfiler/CellProfiler/wiki/Getting-started-using-CellProfiler-from-the-command-line
								cellprofiler -c -r -p ExampleHuman/ExampleHuman.cppipe -o output/ExampleHuman -i ExampleHuman/images
								"""
						},

					]
				}
				export: bash.#Run & {
					input: build.output
					script: contents: ""
					export: {
						directories: {"/usr/local/src/output": _}
					}
				}
			}
		}

		// applied code and/or file formatting
		clean: {
			// code formatting for cuelang
			cue: docker.#Run & {
				input:   _cue_build.output
				workdir: "/workdir"
				command: {
					name: "cue"
					args: ["fmt", "/workdir/project.cue"]
				}
				export: {
					files: "/workdir/project.cue": _
				}
			}
		}

		// export testing coverage details
		coverage: {
			pytest: docker.#Run & {
				input: _python_build.output
				command: {
					name: "poetry"
					args: ["run", "pytest", "--cov=pycytominer_transform", "tests/"]
				}
			}
			coverage: docker.#Run & {
				input: pytest.output
				command: {
					name: "poetry"
					args: ["run", "coverage", "html"]
				}
				export: {
					directories: "/workdir/htmlcov": _
				}
			}
		}

		docs: {
			// check that we don't have sphinx build errors
			sphinx: docker.#Run & {
				input:   _python_build.output
				workdir: "/workdir"
				command: {
					name: "poetry"
					args: ["run", "sphinx-build", "/workdir/docs/source", "/workdir/docs/build"]
				}
				export: directories: "/workdir/docs/build": _
			}
		}

		// various tests for this repo
		test: {
			// python versions to reference for builds
			"3.9": _
			"3.8": _

			[compat_python_version=string]: {

				// python build for tests with specific version as compat_python_version
				build: #PythonBuild & {
					filesystem: client.filesystem."./".read.contents
					python_ver: compat_python_version
					poetry_ver: poetry_version
				}
				// run pre-commit checks
				pre_commit: docker.#Run & {
					input: build.output
					command: {
						name: "poetry"
						args: ["run", "pre-commit", "run", "--all-files"]
					}
				}
				// check that we don't have sphinx build errors
				sphinx: docker.#Run & {
					input:   build.output
					workdir: "/workdir"
					command: {
						name: "poetry"
						args: ["run", "sphinx-build", "/workdir/docs/source", "/tmp/doctest", "-W"]
					}
				}
				// run pytest
				pytest: docker.#Run & {
					input: build.output
					command: {
						name: "poetry"
						args: ["run", "pytest"]
					}
				}
				// check CITATION.cff for proper format
				citation: docker.#Run & {
					input:   build.output
					workdir: "/workdir"
					command: {
						name: "poetry"
						args: ["run", "cffconvert", "--validate"]
					}
				}
			}
		}
	}
}
