import unittest
import json
from app import webserver 
from app.data_ingestor import DataIngestor
import time

class TestWebserver(unittest.TestCase):
    def setUp(self):
        # creează un client de test Flask
        self.client = webserver.test_client()
        self.client.testing = True
        
    def test_num_jobs(self):
        response = self.client.get('/api/num_jobs')
        self.assertEqual(response.status_code, 200)
        data = json.loads(response.data)
        self.assertIn("status", data)

    def test_states_mean(self):
        data = {"question": "Percent of adults aged 18 years and older who have obesity"}
        response = self.client.post('/api/states_mean', json=data)
        self.assertEqual(response.status_code, 200)
        response_json = response.get_json()
        self.assertIn('job_id', response_json)  # Ensure 'job_id' is in the response

        time.sleep(1)
        job_id = response_json['job_id']
        response = self.client.get(f'/api/get_results/{job_id}')
        response_data = response.get_json()["data"]
        self.assertEqual(31.3, response_data["Alabama"])
        self.assertEqual(27.7, response_data["New Mexico"])
        self.assertAlmostEqual((29.4 + 30.8 + 31.6) / 3, response_data["Ohio"], places=3)
        print(response_data)

    def test_state_mean(self):
        data = {"state": "Ohio", "question": "Percent of adults aged 18 years and older who have obesity"}
        response = self.client.post('/api/state_mean', json=data)
        self.assertEqual(response.status_code, 200)
        response_json = response.get_json()
        self.assertIn('job_id', response_json)  # Ensure 'job_id' is in the response

        time.sleep(1)
        job_id = response_json['job_id']
        response = self.client.get(f'/api/get_results/{job_id}')
        response_data = response.get_json()["data"]
        self.assertAlmostEqual((29.4 + 30.8 + 31.6) / 3, response_data["Ohio"], places=3)
        print(response_data)

    def test_global_mean(self):
        data = {"question": "Percent of adults aged 18 years and older who have obesity"}
        response = self.client.post('/api/global_mean', json=data)
        self.assertEqual(response.status_code, 200)
        response_json = response.get_json()
        self.assertIn('job_id', response_json)  # Ensure 'job_id' is in the response

        time.sleep(1)
        job_id = response_json['job_id']
        response = self.client.get(f'/api/get_results/{job_id}')
        response_data = response.get_json()["data"]
        correct_val = (29.4 + 30.8 + 31.6 + 27.7 + 31.3) / 5
        self.assertAlmostEqual(correct_val, response_data["global_mean"], places=3)
        print(response_data)