from abc import ABC, abstractmethod
import logging
import csv

logger = logging.getLogger(__name__)
logger.propagate = False
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


class NetworkDevice:
  def __init__(self, device_id):
    self.device_id = int(device_id)

    # keyed by vlan_id
    self.singular_assignments = {}
    self.redundant_assignments = {}

  def add_assignment(self, assignment):
    vlan_id = assignment.vlan_id
    # logger.debug("device: %s add_assignment: %s", self.device_id, assignment)
    if assignment.primary_port:
      #if vlan_id in self.redundant_assignments:
        # logger.debug("assignment is redundant and already added")
        #self.redundant_assignments[vlan_id].has_matching_primary = True
      #else:
        # logger.debug("assignment is singular so far")
        self.singular_assignments[vlan_id] = assignment
    else:
      # logger.debug("Redundant assignment")
      if vlan_id in self.singular_assignments:
        del self.singular_assignments[vlan_id]
        assignment.has_matching_primary = True
      self.redundant_assignments[vlan_id] = assignment

  def sort_assignments(self):
    self.singular_assignments_sorted = sorted(self.singular_assignments.values(), key=lambda a: a.vlan_id)

    matched_redundant_assignments = [a for a in self.redundant_assignments.values() if a.has_matching_primary]
    self.redundant_assignments_sorted = sorted(matched_redundant_assignments, key=lambda a: a.vlan_id)

  def dump(self):
    logger.debug(self)
    logger.debug("  singular_assignments:")
    for assignment in self.singular_assignments_sorted:
      logger.debug("    %s", assignment)
    logger.debug("  redundant_assignments:")
    for assignment in self.redundant_assignments_sorted:
      logger.debug("    %s", assignment)

  def __repr__(self):
    return "NetworkDevice(device_id=%d)" % self.device_id


class NetworkDevices:
  def __init__(self):
    self._devices = {}

  def get(self, device_id):
    if device_id not in self._devices:
      self._devices[device_id] = NetworkDevice(device_id)
    return self._devices[device_id]

  def dump_devices(self):
    for device_id, device in self._devices.items():
      device.dump()

  def load_devices(self):
    for device_id, device in self._devices.items():
      device.sort_assignments()


class TelnyxBase(ABC):
  def __init__(self, file_name):
    logger.debug("loading file: %s for type: %s", file_name, self.cls)
    self._data = []
    with open(file_name, newline='') as csvfile:
      csvreader = csv.DictReader(csvfile)
      for row in csvreader:
        obj = self.parse_row(row)
        self._data.append(obj)

  @abstractmethod
  def parse_row(self, row):
    pass


class Request:
  def __init__(self, request_id, redundant):
    self.request_id = request_id
    self.redundant = redundant

  def __repr__(self):
    return "Request(id=%d,redundant=%s)" % (self.request_id, self.redundant)


class Requests(TelnyxBase):
  cls = Request

  def parse_row(self, row):
    return Request(int(row['request_id']), (row['redundant'] is '1'))


class Assignment:
  def __init__(self, devices, device_id, primary_port, vlan_id):
    self.device_id = device_id
    self.primary_port = primary_port
    self.vlan_id = vlan_id
    self.has_matching_primary = False

    self.device = devices.get(device_id)
    self.device.add_assignment(self)

  def __repr__(self):
    data = (self.device_id, self.vlan_id, self.primary_port, self.has_matching_primary)
    return "Assignment(device=%d,vlan=%d,primary=%s,matched=%s)" % data


class Assignments(TelnyxBase):
  cls = Assignment

  def __init__(self, file_name):
    self.devices = NetworkDevices()
    super().__init__(file_name)
    self.devices.load_devices()

  def parse_row(self, row):
    return Assignment(self.devices, int(row['device_id']),
                      (row['primary_port'] is '1'), int(row['vlan_id']))


class Reservation:
  def __init__(self, request_id, device_id, primary_port, vlan_id):
    self.request_id = request_id
    self.device_id = device_id
    self.primary_port = primary_port
    self.vlan_id = vlan_id

  def __repr__(self):
    return "Reservation(request=%d,device=%d,primary=%s,vlan=%d)" % (self.request_id, self.device_id, self.primary_port, self.vlan_id)

  def __eq__(self, other):
    return self.__repr__() == other.__repr__()


class Reservations(TelnyxBase):
  cls = Reservation

  def parse_row(self, row):
    return Reservation(int(row['request_id']), int(row['device_id']),
                       (row['primary_port'] is '1'), int(row['vlan_id']))

  def write(reservations, output_filename): 
    with open(output_filename, 'w', newline='') as csvfile:
      csvwriter = csv.DictWriter(csvfile, fieldnames=fieldnames)
      for row in csvreader:
        obj = self.parse_row(row)
        self._data.append(obj)


class RequestProcessor:
  def __init__(self, assignments):
    self.assignments = assignments
    self.devices = assignments.devices
    self.reservations = []

  def run(self, requests):
    for request in requests._data:
      self.reservations.extend(self.process_request(request))
    return self.reservations

  def process_request(self, request):
    if request.redundant:
      assignment = self.process_assignment(request, lambda d: d.redundant_assignments_sorted)
      reservations = [
        Reservation(request.request_id, assignment.device_id, False, assignment.vlan_id),
        Reservation(request.request_id, assignment.device_id, True, assignment.vlan_id)
      ]
      return reservations
    else:
      assignment = self.process_assignment(request, lambda d: d.singular_assignments_sorted)
      reservations = [Reservation(request.request_id, assignment.device_id, True, assignment.vlan_id)]
      return reservations

  def process_assignment(self, request, assignments):
    lowest_vlans = {}
    for device_id, device in self.devices._devices.items():
      if len(assignments(device)) > 0:
        lowest_vlans[device_id] = assignments(device)[0]
    logger.debug("lowest_vlans: %s", lowest_vlans)
    device_id, assignment = min(lowest_vlans.items(), key=lambda x: x[1].vlan_id)
    # logger.debug("device: %d assignment: %s", device_id, assignment)
    assignments(self.devices.get(device_id)).remove(assignment)
    return assignment

if __name__ == '__main__':
    assignments = Assignments('vlans.csv')
    assignments.devices.dump_devices()
    request_processor = RequestProcessor(assignments)
    requests = Requests('requests.csv')

    reservations = request_processor.run(requests)
    Reservations.write(reservations, 'output.csv')
