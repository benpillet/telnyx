import unittest
import logging
from telnyx import *

logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler())


class TestRequest(unittest.TestCase):
  def setUp(self):
    self.assignments = Assignments('test_vlans.csv')
    self.assignments.devices.dump_devices()
    self.request_processor = RequestProcessor(self.assignments)

  def test_redundant_request(self):
    request = Request(0, True)
    actual_reservation = self.request_processor.process_request(request)

    expected_reservations = [
      Reservation(0, 1, False, 1),
      Reservation(0, 1, True, 1)
    ]
    self.assertEqual(actual_reservation, expected_reservations)

  def test_singular_request(self):
    request = Request(1, False)
    actual_reservation = self.request_processor.process_request(request)

    expected_reservations = [Reservation(1, 2, True, 1)]
    self.assertEqual(actual_reservation, expected_reservations)

  def test_csv(self):
    requests = Requests('test_requests.csv')
    logger.debug("requests: %s", requests._data)

    expected_reservations = Reservations('test_output.csv')

    actual_reservations = self.request_processor.run(requests)

    for reservation in expected_reservations._data:
      logger.debug("expected_reservation: %s", reservation)
    for reservation in actual_reservations:
      logger.debug("actual_reservation:   %s", reservation)

    self.assertEqual(actual_reservations, expected_reservations._data)

if __name__ == '__main__':
    unittest.main()
