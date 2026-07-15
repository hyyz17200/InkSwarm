import sys


if __name__ == "__main__":
    if "--spooler-helper" in sys.argv:
        from printfarm.spooler_helper import main as spooler_helper_main

        helper_index = sys.argv.index("--spooler-helper")
        sys.exit(spooler_helper_main(sys.argv[helper_index + 1:]))
    if "--print-helper" in sys.argv:
        from printfarm.print_helper import main as print_helper_main

        helper_index = sys.argv.index("--print-helper")
        sys.exit(print_helper_main(sys.argv[helper_index + 1:]))
    from printfarm.gui import run

    run()
