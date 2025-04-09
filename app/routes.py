from app import webserver
from flask import request, jsonify

import json

# Example endpoint definition
@webserver.route('/api/post_endpoint', methods=['POST'])

def post_endpoint():
    """
    Handles POST requests to the '/api/post_endpoint' route.

    This function processes incoming JSON data from POST requests, logs the received data,
    and sends back a JSON response containing a success message and the received data.

    Returns:
        Response: A JSON response with a success message and the received data if the request
        method is POST. Otherwise, returns a JSON response with an error message and a 405 status code.
    """
    if request.method == 'POST':
        # Assuming the request contains JSON data
        data = request.json
        print(f"got data in post {data}")

        # Process the received data
        # For demonstration purposes, just echoing back the received data
        response = {"message": "Received data successfully", "data": data}

        # Sending back a JSON response
        return jsonify(response)
    return jsonify({"error": "Method not allowed"}), 405

@webserver.route('/api/get_results/<job_id>', methods=['GET'])
def get_response(job_id):
    """
    Handles the response for a given job ID by checking its status and
        returning the appropriate JSON response.

    Args:
        job_id (str): The ID of the job to retrieve the response for.

    Returns:
        flask.Response: A JSON response indicating the status of the job

    Raises:
        ValueError: If the job_id cannot be converted to an integer.
        FileNotFoundError: If the result file for a completed job is not found.
        JSONDecodeError: If the result file contains invalid JSON.
    """
    webserver.logger.info("IN /api/get_results/%s", job_id)
    if int(job_id) >= webserver.job_counter:
        webserver.logger.error("ERROR in /api/get_results/%s - Reason:Invalid job_id", job_id)
        return jsonify({"status": "error", "reason": "Invalid job_id"})
    if webserver.tasks_runner.jobs_status[int(job_id)] == "done":
        with open(f"results/{job_id}.json", 'r', encoding= "utf-8") as file:
            data = json.load(file)  # Load JSON content from the file
            webserver.logger.info("OUT in /api/get_results/%s - data: %s", job_id, data)
            return jsonify({"status": "done", "data": data})
    # If not, return running status
    webserver.logger.info("OUT in /api/get_results/%s - status: Running", job_id)
    return jsonify({'status': 'running'})

@webserver.route('/api/states_mean', methods=['POST'])
def states_mean_request():
    """
    Handles a POST request to calculate the mean of states data.

    This function retrieves JSON data from the incoming request, logs the data,
    assigns a unique job ID, and queues the job for processing with the "states_mean" task type.
    It then increments the job counter and returns the job ID in the response.

    Returns:
        Response: A JSON response containing the job ID and an HTTP status code of 200.
    """
    # Get request data
    data = request.json
    webserver.logger.info("IN /api/states_mean - data: %s", data)

    job_id = webserver.job_counter
    if webserver.tasks_runner.add_job(job_id, data, "states_mean") == "stoping":
        webserver.logger.error("ERROR in /api/states_mean - Reason:shutting down")
        return jsonify({"status": "error", "reason": "shutting down"})

    webserver.logger.info("OUT /api/states_mean - job_id: %d", job_id)
    webserver.job_counter += 1
    return jsonify({"job_id": job_id}), 200


@webserver.route('/api/state_mean', methods=['POST'])
def state_mean_request():
    """
    Handles a request to calculate the mean of a state from the provided data.

    This function retrieves JSON data from the incoming request, assigns a unique
    job ID, and adds the job to the task runner for processing. The job is identified
    by the "state_mean" operation type.

    Returns:
        Response: A JSON response containing the assigned job ID and an HTTP status code of 200.
    """
    data = request.json
    webserver.logger.info("IN /api/state_mean - data: %s", data)

    job_id = webserver.job_counter
    if webserver.tasks_runner.add_job(job_id, data, "state_mean") == "stoping":
        webserver.logger.error("ERROR in /api/state_mean - Reason:shutting down")
        return jsonify({"status": "error", "reason": "shutting down"})

    webserver.logger.info("OUT /api/state_mean - job_id: %d", job_id)
    webserver.job_counter += 1
    return jsonify({"job_id": job_id}), 200


@webserver.route('/api/best5', methods=['POST'])
def best5_request():
    """
    Handles the "best5" request by registering a job with the provided data.

    This function retrieves JSON data from the incoming request, assigns a unique
    job ID, and registers the job in the task runner without waiting for the task
    to complete. It then increments the job counter and returns the job ID in the
    response.

    Returns:
        Response: A JSON response containing the assigned job ID and an HTTP status code 200.
    """
    # Get request data
    data = request.json
    webserver.logger.info("IN /api/best5- data: %s", data)

    job_id = webserver.job_counter
    if webserver.tasks_runner.add_job(job_id, data, "best5") == "stoping":
        webserver.logger.error("ERROR in /api/best5 - Reason:shutting down")
        return jsonify({"status": "error", "reason": "shutting down"})

    webserver.logger.info("OUT /api/best5 - job_id: %d", job_id)
    webserver.job_counter += 1
    return jsonify({"job_id": job_id}), 200

@webserver.route('/api/worst5', methods=['POST'])
def worst5_request():
    """
    Handles the 'worst5' request by registering a job for processing the data.

    This function retrieves JSON data from the incoming request, registers a new job
    with the task runner, and increments the job counter. It does not wait for the 
    task to finish and immediately returns a response containing the associated job ID.

    Returns:
        Response: A JSON response with the job ID and a status code of 200.
    """
    # Get request data
    data = request.json
    webserver.logger.info("IN /api/worst5 - data: %s", data)

    job_id = webserver.job_counter
    if webserver.tasks_runner.add_job(job_id, data, "worst5") == "stoping":
        webserver.logger.error("ERROR in /api/worst5 - Reason:shutting down")
        return jsonify({"status": "error", "reason": "shutting down"})

    webserver.logger.info("OUT /api/worst5 - job_id: %d", job_id)
    webserver.job_counter += 1
    return jsonify({"job_id": job_id}), 200

@webserver.route('/api/global_mean', methods=['POST'])
def global_mean_request():
    """
    Handles a POST request to initiate a global mean computation task.

    This function retrieves the JSON data from the incoming request, registers
    a new job for the global mean computation task, and returns a unique job ID
    associated with the task. The task is added to the task runner and executed
    asynchronously.

    Returns:
        Response: A JSON response containing the unique job ID and an HTTP status code 200.
    """
    # Get request data
    data = request.json
    webserver.logger.info("IN /api/global_mean - data: %s", data)

    job_id = webserver.job_counter
    if webserver.tasks_runner.add_job(job_id, data, "global_mean") == "stoping":
        webserver.logger.error("ERROR in /api/global_mean - Reason:shutting down")
        return jsonify({"status": "error", "reason": "shutting down"})

    webserver.logger.info("OUT /api/global_mean - job_id: %d", job_id)
    webserver.job_counter += 1
    return jsonify({"job_id": job_id}), 200

@webserver.route('/api/diff_from_mean', methods=['POST'])
def diff_from_mean_request():
    """
    Handles a request to calculate the difference from the mean for a given dataset.

    This function retrieves JSON data from the incoming request, registers a new job 
    to process the data asynchronously, and returns a unique job ID to the client.

    Returns:
        Response: A JSON response containing the unique job ID and an HTTP status code 200.
    """
    # Get request data
    data = request.json
    webserver.logger.info("IN /api/diff_from_mean - data: %s", data)

    job_id = webserver.job_counter
    if webserver.tasks_runner.add_job(job_id, data, "diff_from_mean") == "stoping":
        webserver.logger.error("ERROR in /api/diff_from_mean - Reason:shutting down")
        return jsonify({"status": "error", "reason": "shutting down"})

    webserver.logger.info("OUT /api/diff_from_mean - job_id: %d", job_id)
    webserver.job_counter += 1
    return jsonify({"job_id": job_id}), 200

@webserver.route('/api/state_diff_from_mean', methods=['POST'])
def state_diff_from_mean_request():
    """
    Handles a request to calculate the difference of a state from the mean.

    This function processes incoming JSON data from the request, assigns a unique
    job ID, and queues the job for execution by the tasks runner. The job is
    identified by the "state_diff_from_mean" operation type.

    Returns:
        Response: A JSON response containing the assigned job ID and an HTTP status code 200.
    """
    data = request.json
    webserver.logger.info("IN /api/state_mean - data: %s", data)

    job_id = webserver.job_counter
    if webserver.tasks_runner.add_job(job_id, data, "state_diff_from_mean") == "stoping":
        webserver.logger.error("ERROR in /api/state_diff_from_mean - Reason:shutting down")
        return jsonify({"status": "error", "reason": "shutting down"})

    webserver.logger.info("OUT /api/state_diff_from_mean - job_id: %d", job_id)
    webserver.job_counter += 1
    return jsonify({"job_id": job_id}), 200

@webserver.route('/api/mean_by_category', methods=['POST'])
def mean_by_category_request():
    """
    Handles a request to calculate the mean by category.

    This function processes a JSON request containing data, assigns a unique job ID,
    and queues the task for execution. The job ID is incremented for each new request.
    The function returns a JSON response with the assigned job ID and a 200 HTTP status code.

    Returns:
        Response: A JSON response containing the job ID and an HTTP status code of 200.
    """
    data = request.json
    webserver.logger.info("IN /api/mean_by_category - data: %s", data)
    
    job_id = webserver.job_counter
    if webserver.tasks_runner.add_job(job_id, data, "mean_by_category") == "stoping":
        webserver.logger.error("ERROR in /api/mean_by_category - Reason:shutting down")
        return jsonify({"status": "error", "reason": "shutting down"})

    webserver.logger.info("OUT /api/mean_by_category - job_id: %d", job_id)
    webserver.job_counter += 1
    return jsonify({"job_id": job_id}), 200

@webserver.route('/api/state_mean_by_category', methods=['POST'])
def state_mean_by_category_request():
    """
    Handles a request to calculate the mean of a state by category.

    This function expects a JSON payload in the request body containing the necessary
    data for the computation. It assigns a unique job ID to the task, adds the task
    to the task runner for processing, and increments the job counter. The response
    includes the assigned job ID.

    Returns:
        Response: A JSON response containing the job ID and a status code of 200.
    """
    data = request.json
    webserver.logger.info("IN /api/state_mean_by_cateogory - data: %s", data)

    job_id = webserver.job_counter
    if webserver.tasks_runner.add_job(job_id, data, "state_mean_by_category") == "stoping":
        webserver.logger.error("ERROR in /api/state_mean_by_category - Reason:shutting down")
        return jsonify({"status": "error", "reason": "shutting down"})

    webserver.logger.info("OUT /api/state_mean_by_category - job_id: %d", job_id)
    webserver.job_counter += 1
    return jsonify({"job_id": job_id}), 200

@webserver.route('/api/jobs', methods=['GET'])
def get_jobs():
    webserver.logger.info("IN /api/jobs")
    data = {f"job_id_{job_id}": status for job_id, status in webserver.tasks_runner.jobs_status.items()}
    webserver.logger.info("OUT /api/jobs - data: %s", data)
    return jsonify({"status": "done", "data": data})

@webserver.route('/api/num_jobs', methods=['GET'])
def get_num_jobs():
    webserver.logger.info("IN /api/num_jobs")
    num_jobs = len({k: v for k, v in webserver.tasks_runner.jobs_status.items() if v == "running"})
    webserver.logger.info("OUT /api/num_jobs - data: %d", num_jobs)
    return jsonify({"status": num_jobs})

@webserver.route('/api/graceful_shutdown', methods=['GET'])
def get_graceful_shutdown():
    webserver.logger.info("IN /api/graceful_shutdown")
    webserver.tasks_runner.stop()
    if webserver.tasks_runner.queue.empty():
        webserver.logger.info("OUT /api/graceful_shutdown - status: Done",)
        return jsonify({"status": "done"})

    webserver.logger.info("OUT /api/graceful_shutdown - status: Running",)
    return jsonify({"status": "running"})

@webserver.route('/index')
def index():
    routes = get_defined_routes()
    msg = f"Hello, World!\n Interact with the webserver using one of the defined routes:\n"

    # Display each route as a separate HTML <p> tag
    paragraphs = ""
    for route in routes:
        paragraphs += f"<p>{route}</p>"

    msg += paragraphs
    return msg

def get_defined_routes():
    routes = []
    for rule in webserver.url_map.iter_rules():
        methods = ', '.join(rule.methods)
        routes.append(f"Endpoint: \"{rule}\" Methods: \"{methods}\"")
    return routes
