import unittest
import logging
from telnyx import *

logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())

class TestRequest(unittest.TestCase):
  def test_redundant_request(self):
    assignments = Assignment.load('test_vlans.csv')
    NetworkDevices.dump_devices()

    request_processor = RequestProcessor(assignments)

    request = Request(0, True)
    actual_reservation = request_processor.process_request(request)
    expected_reservations = [
      Reservation(0, 1, False, 1),
      Reservation(0, 1, True, 1)
    ]
    self.assertEqual(actual_reservation, expected_reservations)

  def test_singular_request(self):
    assignments = Assignment.load('test_vlans.csv')
    NetworkDevices.dump_devices()

    request_processor = RequestProcessor(assignments)

    request = Request(1, False)
    actual_reservation = request_processor.process_request(request)
    expected_reservations = [Reservation(1, 2, True, 1)]
    self.assertEqual(actual_reservation, expected_reservations)

  def test_csv(self):
    assignments = Assignment.load('test_vlans.csv')
    logger.debug("assignments: %s", Assignment._data)
    NetworkDevices.dump_devices()

    requests = Request.load('test_requests.csv')
    logger.debug("requests: %s", Request._data)

    expected_reservations = Reservation.load('test_output.csv')

    request_processor = RequestProcessor(assignments)
    actual_reservations = request_processor.run(requests)

    for reservation in expected_reservations:
      logger.debug("expected_reservation: %s", reservation)
    for reservation in actual_reservations:
      logger.debug("actual_reservation:   %s", reservation)
    
    self.assertEqual(actual_reservations, expected_reservations)

if __name__ == '__main__':
    unittest.main()
