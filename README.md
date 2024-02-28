# Solution of MIT 6.5840 (Distributed Systems) Labs

This repository provides a high performance and fully tested solution to `2023 version` of MIT 6.5840 course. I have written a blog about some concurrency and design decisions for implementing Raft in Go that you can find it [in this link](https://farzad1132.github.io/post/raft-implementation-go/).

TAs of the course have provided some tools for printing logs and running test. I have updated and packaged them to be used easier.


## Implementation status

- [x] Lab 2A
- [x] Lab 2B
- [x] Lab 2C
- [x] Lab 2D
- [ ] Lab 3A
- [ ] Lab 3B
- [ ] Lab 4A
- [ ] Lab 4B

## Test results
<pre>┏━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━┓
┃<b> Test                  </b>┃<b> Failed </b>┃<b> Total </b>┃<b>        Time </b>┃
┡━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━┩
│ <font color="#4E9A06">TestInitialElection2A</font> │      0 │    50 │ 3.89 ± 0.23 │
│ <font color="#4E9A06">TestReElection2A</font>      │      0 │    50 │ 6.17 ± 0.42 │
│ <font color="#4E9A06">TestManyElections2A</font>   │      0 │    50 │ 6.64 ± 0.70 │
└───────────────────────┴────────┴───────┴─────────────┘</pre>

<pre>┏━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃<b> Test                   </b>┃<b> Failed </b>┃<b> Total </b>┃<b>         Time </b>┃
┡━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ <font color="#4E9A06">TestBasicAgree2B</font>       │      0 │    50 │  1.19 ± 0.15 │
│ <font color="#4E9A06">TestRPCBytes2B</font>         │      0 │    50 │  2.06 ± 0.17 │
│ <font color="#4E9A06">TestFollowerFailure2B</font>  │      0 │    50 │  5.20 ± 0.16 │
│ <font color="#4E9A06">TestLeaderFailure2B</font>    │      0 │    50 │  5.77 ± 0.15 │
│ <font color="#4E9A06">TestFailAgree2B</font>        │      0 │    50 │  5.75 ± 0.86 │
│ <font color="#4E9A06">TestFailNoAgree2B</font>      │      0 │    50 │  4.03 ± 0.12 │
│ <font color="#4E9A06">TestConcurrentStarts2B</font> │      0 │    50 │  1.39 ± 0.16 │
│ <font color="#4E9A06">TestRejoin2B</font>           │      0 │    50 │  5.66 ± 0.75 │
│ <font color="#4E9A06">TestBackup2B</font>           │      0 │    50 │ 17.97 ± 0.87 │
│ <font color="#4E9A06">TestCount2B</font>            │      0 │    50 │  2.90 ± 0.16 │
└────────────────────────┴────────┴───────┴──────────────┘</pre>

<pre>┏━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃<b> Test                    </b>┃<b> Failed </b>┃<b> Total </b>┃<b>         Time </b>┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ <font color="#4E9A06">TestPersist12C</font>          │      0 │    50 │  5.07 ± 0.18 │
│ <font color="#4E9A06">TestPersist22C</font>          │      0 │    50 │ 18.04 ± 0.93 │
│ <font color="#4E9A06">TestPersist32C</font>          │      0 │    50 │  2.51 ± 0.09 │
│ <font color="#4E9A06">TestFigure82C</font>           │      0 │    50 │ 32.71 ± 2.27 │
│ <font color="#4E9A06">TestUnreliableAgree2C</font>   │      0 │    50 │  2.43 ± 0.22 │
│ <font color="#4E9A06">TestFigure8Unreliable2C</font> │      0 │    50 │ 33.96 ± 2.11 │
│ <font color="#4E9A06">TestReliableChurn2C</font>     │      0 │    50 │ 17.40 ± 1.12 │
│ <font color="#4E9A06">TestUnreliableChurn2C</font>   │      0 │    50 │ 16.52 ± 0.16 │
└─────────────────────────┴────────┴───────┴──────────────┘</pre>

<pre>┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━┓
┃<b> Test                            </b>┃<b> Failed </b>┃<b> Total </b>┃<b>         Time </b>┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━┩
│ <font color="#4E9A06">TestSnapshotBasic2D</font>             │      0 │    50 │  4.96 ± 0.20 │
│ <font color="#4E9A06">TestSnapshotInstall2D</font>           │      0 │    50 │ 42.97 ± 0.99 │
│ <font color="#4E9A06">TestSnapshotInstallUnreliable2D</font> │      0 │    50 │ 53.03 ± 1.70 │
│ <font color="#4E9A06">TestSnapshotInstallCrash2D</font>      │      0 │    50 │ 32.43 ± 0.31 │
│ <font color="#4E9A06">TestSnapshotInstallUnCrash2D</font>    │      0 │    50 │ 38.78 ± 1.34 │
│ <font color="#4E9A06">TestSnapshotAllCrash2D</font>          │      0 │    50 │ 10.17 ± 0.82 │
│ <font color="#4E9A06">TestSnapshotInit2D</font>              │      0 │    50 │  3.64 ± 0.16 │
└─────────────────────────────────┴────────┴───────┴──────────────┘</pre>

Above graphics are the result of `dstest` CLI tool that is included in this repo. 

## Installation

### Prerequisites
- `Go version 1.21.4` for running Raft and tests (It might work on other versions, but I have not tested it on any other version)
- `Python 3.8` for running testing and printing CLIs

### Getting the code
Just run the following command in a directory with a *nix terminal (If you are using Window, run this in git bash or WSL)

```bash
git clone git@github.com:farzad1132/mit-6.5840-lab-implementation.git
```

### Running Raft tests
1. First go to `src/raft` directory.
2. run the following command for running a test (more info in the [Lab's page](http://nil.csail.mit.edu/6.5840/2023/labs/lab-raft.html))

```bash
go test -run <Regex>
```
Replace `<Regex>` with a part of test names to run those tests. For example this can be `TestPersist12C` or `2B`. (Don't forget about `-timeout` and `-race`)
3. If you want to turn the logs on run the following command

```bash
VERBOSE=1 go test -run <Regex> > <log-filename>
```
In this command, we are redirecting the output of the test (because its might be pretty long) to a file named `<log-filename>`

### Installing python CLIs

I strongly recommend you to install these CLIs in an isolated python environment.

When you have activated your virtual environment, go to `python_setup` directory and run the following command:

```bash
pip install --editable .
```

The `--editable` is to install the package in development mode. In this case, if you want to change the source code of command, the is no need for reinstalling them.

## Notes on python CLI tools

After installing python CLIs as explained above, you will have access to two cLI commands: `pretty` and `dstest`.

These two tools was originally developed by Jose Javier in [this blog post](https://blog.josejg.com/debugging-pretty/) and I have just packaged and updated them.