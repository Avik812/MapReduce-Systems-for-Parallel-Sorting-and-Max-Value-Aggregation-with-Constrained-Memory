if __name__ == "__main__":
    from multiprocessing import set_start_method
    try:
        set_start_method("spawn")
    except RuntimeError:
        pass

    from runner import run_experiments
    run_experiments()