from queue import Queue
from threading import Thread, Event
import time
import os
import json
from app.data_ingestor import DataIngestor
from collections import defaultdict
import statistics
class ThreadPool:
    """
    ThreadPool is a class that manages a pool of threads to execute tasks concurrently.

    Attributes:
        queue (Queue): A thread-safe queue to hold tasks to be executed by the thread pool.
        threads_event (Event): An event used to signal threads to stop execution.
        threads (list): A list to store the threads in the pool.
        number_of_threads (int): The number of threads in the pool,
            determined by an environment variable or hardware concurrency.
        jobs_status (dict): A dictionary to track the status of jobs,
                                mapping job IDs to their status ("running" or "done").
        data_ingestor (DataIngestor): An instance of DataIngestor used for data processing.
        accepting_jobs: An observator for whether we accept jobs or not
    Methods:
        start():
            Initializes and starts the threads in the thread pool.
        stop():
            Signals all threads to stop and waits for them to finish execution.
        get_total_threads():
            Determines the total number of threads to use based on the
                TP_NUM_OF_THREADS environment variable or hardware concurrency.
        add_job(job_id, data, request):
            Adds a new job to the queue and marks its status as "running".

    Notes:
        - Threads are reused for tasks and are not recreated for each task.
        - The TP_NUM_OF_THREADS environment variable specifies the number of threads to use.
    """
    def __init__(self, data_ingestor):
        self.queue = Queue()
        self.threads_event = Event()
        self.threads = []
        self.number_of_threads = self.get_total_threads()
        self.jobs_status = {}
        self.accepting_jobs = True
        self.data_ingestor = data_ingestor

    def start(self):
        """
        Starts the task runner by initializing and starting the specified number of threads.

        This method creates a new `TaskRunner` thread for each thread in `self.number_of_threads`.
        The threads are started and appended to the `self.threads` list for tracking.
        """
        for _ in range(self.number_of_threads):
            thread = TaskRunner(self.threads_event,
                                self.queue,
                                self.jobs_status,
                                self.data_ingestor)
            thread.start()
            self.threads.append(thread)
    def stop(self):
        """
        Stops all running threads managed by this instance.

        This method sets the event flag to signal all threads to stop their execution.
        It then waits for each thread to complete by calling `join` on them.
        """
        self.accepting_jobs = False
        self.queue.join()
        self.threads_event.set()
        for thread in self.threads:
            thread.join()

    def get_total_threads(self):
        """
        Retrieves the total number of threads to be used by the application.

        The method checks for the environment variable 'TP_NUM_OF_THREADS' to determine
        the number of threads. If the environment variable is not set, it defaults to
        the number of CPU cores available on the system.

        Returns:
            int: The total number of threads to be used.
        """
        default_value = os.cpu_count()
        total_threads = os.environ.get('TP_NUM_OF_THREADS', default_value)
        return int(total_threads)
    def add_job(self, job_id, data, request):
        """
        Adds a new job to the queue and sets its initial status to "running".

        Args:
            job_id (str): A unique identifier for the job.
            data (Any): The data associated with the job.
            request (Any): The request object containing additional job details.

        Returns:
            - "stoping" if the server itself is graceful shutdowning
            - "done" if operation is been done successfully
        """
        if not self.accepting_jobs:
            return "stoping"

        self.jobs_status[job_id] = "running"
        block = job_id, data, request
        self.queue.put(block)
        return "done"

class TaskRunner(Thread):
    def __init__(self, threads_event, queue, jobs_status, data_ingestor):
        super().__init__()
        self.threads_event = threads_event
        self.queue = queue
        self.jobs_status = jobs_status
        self.data_ingestor = data_ingestor

    def run(self):
        while not self.threads_event.is_set():
            job_id, data, request = self.queue.get()
            response = self.solver(data, request)

            with open("results/" + str(job_id) + ".json", mode= 'w', encoding="utf-8") as file:
                json.dump(response, file)

            self.jobs_status[job_id] = "done"
            self.queue.task_done()

    def solver(self, data, request):
        """
        Processes a given request by matching it to the appropriate solver method.

        Args:
            data (any): The input data required for solving the request.
            request (str): The type of request to process. Supported values are:
                - "states_mean": Calculate the mean for all states.
                - "state_mean": Calculate the mean for a specific state.
                - "global_mean": Calculate the global mean.
                - "best5": Retrieve the top 5 best-performing entities.
                - "worst5": Retrieve the bottom 5 worst-performing entities.
                - "diff_from_mean": Calculate the difference from the global mean.
                - "state_diff_from_mean": Calculate the difference from the mean for
                    a specific state.
                - "mean_by_category": Calculate the mean grouped by a specific category.
                - "state_mean_by_category": Calculate the mean grouped by category for
                    a specific state.

        Returns:
            any: The result of the corresponding solver method based on the request.
                  Returns "error" if the request type is unsupported.
        """
        match request:
            case "states_mean":
                return self.solve_states_mean(data)
            case "state_mean":
                return self.solve_state_mean(data)
            case "global_mean":
                return self.solve_global_mean(data)
            case "best5":
                return self.solve_best5(data)
            case "worst5":
                return self.solve_worst5(data)
            case "diff_from_mean":
                return self.solve_diff_from_mean(data)
            case "state_diff_from_mean":
                return self.solve_state_diff_from_mean(data)
            case "mean_by_category":
                return self.solve_mean_by_category(data)
            case "state_mean_by_category":
                return self.solve_state_mean_by_category(data)
            case _:
                return "error"
        return 'done'
    def solve_states_mean(self, data):
        """
        Computes the mean of specific data values grouped by state
            and returns the results sorted by mean value.

        Args:
            data (dict): A dictionary containing a "question" key, whose value is used
            to filter rows from the ingested data.

        Returns:
            dict: A dictionary where the keys are state identifiers and the values are the mean 
            of the corresponding data values, sorted in ascending order of the mean values.

        Notes:
            - The method assumes that `self.data_ingestor.data` is a list of rows,
                where each row is a list-like structure containing the relevant data.
            - The state identifier is expected to be at index 4 of each row.
            - The data value to compute the mean is expected to be at index 11 of each row.
            - Rows with empty or non-numeric data values at index 11 are ignored.
        """
        question = data["question"]
        data_to_search = [row for row in self.data_ingestor.data if question in row]
        state_data = defaultdict(list)
        for row in data_to_search:
            state = row[4]
            data_value = row[11]
            if data_value.strip():
                state_data[state].append(float(data_value))

        mean_per_state = {state: statistics.mean(values) for state, values in state_data.items()}
        return dict(sorted(mean_per_state.items(), key=lambda x: x[1]))
    def solve_state_mean(self, data):
        """
        Computes the mean of specific values for a given state based on the provided data.

        Args:
            data (dict): A dictionary containing:
                - "question" (str): The question to filter the data.
                - "state" (str): The state to filter the data.

        Returns:
            dict: A dictionary where the key is the state and the value is the mean of the 
                  filtered values corresponding to the given question and state.

        Raises:
            ValueError: If no matching data is found for the given question and state.
        """
        question = data["question"]
        state = data["state"]
        data_to_search = [row for row in self.data_ingestor.data if question in row and state in row]
        values = [float(row[11]) for row in data_to_search]
        return {state: statistics.mean(values)}
    def solve_global_mean(self, data):
        """
        Computes the global mean of specific values from the ingested data based on a given question.

        Args:
            data (dict): A dictionary containing the key "question", which specifies the 
                         question to filter the data rows.

        Returns:
            dict: A dictionary containing the key "global_mean" with the computed mean value 
                  of the filtered data.

        Raises:
            ValueError: If the filtered data contains no values to compute the mean.
        """
        question = data["question"]
        data_to_search = [row for row in self.data_ingestor.data if question in row]
        values = [float(row[11]) for row in data_to_search]
        return {"global_mean": statistics.mean(values)}
    def solve_best5(self, data):
        """
        Computes the top 5 states based on the mean of a specific data value 
        associated with a given question.

        Args:
            data (dict): A dictionary containing the following keys:
                - "question" (str): The question to filter the data by.

        Returns:
            dict: A dictionary containing the top 5 states as keys and their 
            corresponding mean values as values. The sorting order (ascending 
            or descending) is determined by whether the question is in 
            `self.data_ingestor.questions_best_is_max`.

        Notes:
            - The method filters rows from `self.data_ingestor.data` where the 
              question is present.
            - It calculates the mean of the data values (column index 11) for 
              each state (column index 4).
            - If the question is in `self.data_ingestor.questions_best_is_max`, 
              the results are sorted in descending order; otherwise, they are 
              sorted in ascending order.
            - Only the top 5 states are included in the returned dictionary.
        """
        question = data["question"]
        data_to_search = [row for row in self.data_ingestor.data if question in row]
        state_data = defaultdict(list)
        for row in data_to_search:
            state = row[4]
            data_value = row[11]
            if data_value.strip():
                state_data[state].append(float(data_value))

        mean_per_state = {state: statistics.mean(values) for state, values in state_data.items()}

        reverse = False
        if question in self.data_ingestor.questions_best_is_max:
            reverse = True

        return dict(sorted(mean_per_state.items(), key=lambda x: x[1], reverse=reverse)[:5])
    def solve_worst5(self, data):
        """
        Identifies the 5 states with the worst (lowest or highest, depending on the context) 
        mean values for a specific question from the ingested data.

        Args:
            data (dict): A dictionary containing the question to search for. 
                         Expected format: {"question": <question_string>}.

        Returns:
            dict: A dictionary containing the 5 states with the worst mean values 
                  for the specified question. The keys are state names, and the 
                  values are the corresponding mean values, sorted in ascending 
                  or descending order based on the context.

        Notes:
            - The function calculates the mean of the data values for each state 
              where the question matches.
            - If the question is in `self.data_ingestor.questions_best_is_max`, 
              the sorting is done in ascending order (lower values are worse). 
              Otherwise, sorting is done in descending order (higher values are worse).
            - Only rows with non-empty data values are considered.
        """
        question = data["question"]
        data_to_search = [row for row in self.data_ingestor.data if question in row]
        state_data = defaultdict(list)
        for row in data_to_search:
            state = row[4]
            data_value = row[11]
            if data_value.strip():
                state_data[state].append(float(data_value))

        mean_per_state = {state: statistics.mean(values) for state, values in state_data.items()}

        reverse = True
        if question in self.data_ingestor.questions_best_is_max:
            reverse = False

        return dict(sorted(mean_per_state.items(), key=lambda x: x[1], reverse=reverse)[:5])
    def solve_diff_from_mean(self, data):
        """
        Calculate the difference between the global mean and the mean for each state.

        Args:
            data (dict): A dictionary where keys represent states and values
                are lists of numerical data.

        Returns:
            dict: A dictionary where keys are states and values are the differences 
                  between the global mean and the mean of the respective state.
        """
        global_mean = self.solve_global_mean(data)["global_mean"]
        states_mean = self.solve_states_mean(data)
        differences = {state: global_mean - mean for state, mean in states_mean.items()}
        return differences
    def solve_state_diff_from_mean(self, data):
        """
        Calculate the difference between the global mean and the mean for each state.

        This method computes the global mean using `solve_global_mean` and the mean for each state
        using `solve_state_mean`. It then calculates the difference between the global mean and 
        each state's mean.

        Args:
            data (dict): A dictionary containing the data required to compute the means. 
                         The structure of the data should be compatible with the methods 
                         `solve_global_mean` and `solve_state_mean`.

        Returns:
            dict: A dictionary where the keys are state identifiers and the values are the 
                  differences between the global mean and the mean for each state.
        """
        global_mean = self.solve_global_mean(data)["global_mean"]
        state_mean = self.solve_state_mean(data)
        difference = {state: global_mean - mean for state, mean in state_mean.items()}
        return difference
    def solve_mean_by_category(self, data):
        """
        Computes the mean of numeric values grouped by state, stratification category, 
        and stratification value for rows matching a specific question.

        Args:
            data (dict): A dictionary containing the key "question", which specifies 
                         the question to filter rows in the dataset.

        Returns:
            dict: A dictionary where keys are tuples in string format 
                  (state, stratification category, stratification value) and values 
                  are the mean of numeric values for the corresponding group.

        Notes:
            - The function filters rows in the dataset where the "question" is present.
            - Rows with invalid numeric values or missing stratification category/value 
              are ignored.
            - The dataset is expected to be accessible via `self.data_ingestor.data`.
            - The column indices for state, stratification category, stratification value, 
              and numeric values must match the dataset structure.
        """
        question = data["question"]
        data_to_search = [row for row in self.data_ingestor.data if question in row]

        category_values = defaultdict(list)

        for row in data_to_search:
            state = row[4]
            strat_cat = row[30]
            strat_val = row[31]
            value = float(row[11])

            if strat_cat == "" or strat_val == "":
                continue
            key = (state, strat_cat, strat_val)
            category_values[key].append(value)

        result = {str(k): statistics.mean(v) for k, v in category_values.items() if v}
        return result
    def solve_state_mean_by_category(self, data):
        """
        Computes the mean of numeric values grouped by stratification category and value
        for a specific state and question.

        Args:
            data (dict): A dictionary containing the following keys:
                - "question" (str): The question to filter rows by.
                - "state" (str): The state to filter rows by.

        Returns:
            dict: A dictionary where the key is the state and the value is another dictionary.
                  The inner dictionary maps a tuple of (stratification category,stratification val)
                  to the mean of the numeric values for that group.

        Notes:
            - The function filters rows from `self.data_ingestor.data` based on the provided
              question and state.
            - It expects the stratification category, stratification value, and numeric value
              to be located at specific indices in the row (30, 31, and 11 respectively).
            - Rows with invalid numeric values are ignored.
        """
        question = data["question"]
        state = data["state"]
        data_to_search = [row for row in self.data_ingestor.data if question in row and state in row]

        category_values = defaultdict(list)

        for row in data_to_search:
            strat_cat = row[30]
            strat_val = row[31]
            value = float(row[11])

            key = (strat_cat, strat_val)
            category_values[key].append(value)

        result = {str(k): statistics.mean(v) for k, v in category_values.items() if v}
        return {state : result}
    