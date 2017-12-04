from collections import defaultdict
import logging
import csv
import operator

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
    logger.debug("device: %s add_assignment vlan: %s primary: %s", self.device_id, vlan_id, assignment.primary_port)
    if assignment.primary_port:
      if vlan_id in self.redundant_assignments:
        logger.debug("assignment is redundant and already added")
      else:
        logger.debug("assignment is singular so far")
        self.singular_assignments[vlan_id] = assignment
    else:
      logger.debug("Redundant assignment")
      if vlan_id in self.singular_assignments:
        del self.singular_assignments[vlan_id]
      self.redundant_assignments[vlan_id] = assignment

  def prep_for_requests(self):
    by_vlan_id = lambda assignment: assignment.vlan_id

    self.singular_assignments_sorted = sorted(self.singular_assignments.values(), key=lambda a: a.vlan_id)
    self.redundant_assignments_sorted = sorted(self.redundant_assignments.values(), key=lambda a: a.vlan_id)

  def dump(self):
    logger.info(self)
    logger.info("  singular_assignments:")
    for assignment in self.singular_assignments_sorted:
      logger.info("    %s", assignment)
    logger.info("  redundant_assignments:")
    for assignment in self.redundant_assignments_sorted:
      logger.info("    %s", assignment)

  def __repr__(self):
    return "NetworkDevice(device_id=%d)" % self.device_id

class NetworkDevices:
  _devices = {}

  @classmethod
  def get(cls, device_id):
    if device_id not in cls._devices:
      cls._devices[device_id] = NetworkDevice(device_id)
    return cls._devices[device_id]

  @classmethod
  def dump_devices(cls):
    for device_id, device in cls._devices.items():
      device.dump()

  @classmethod
  def prep_devices(cls):
    for device_id, device in NetworkDevices._devices.items():
      device.prep_for_requests()

class TelnyxBase:
  @classmethod
  def load(cls, file_name):
    logger.debug("loading file: %s for type: %s", file_name, cls)
    with open(file_name, newline='') as csvfile:
      csvreader = csv.DictReader(csvfile)
      for row in csvreader:
        obj = cls.parse_row(row)
        cls._data.append(obj)
    return cls._data

class Request(TelnyxBase):
  _data = []

  def __init__(self, request_id, redundant):
    self.request_id = request_id
    self.redundant = redundant

  @classmethod
  def parse_row(cls, row):
    return Request(int(row['request_id']), (row['redundant'] is '1'))

  def __repr__(self):
    return "Request(id=%d,redundant=%s)" % (self.request_id, self.redundant)

class Assignment(TelnyxBase):
  _data = []

  def __init__(self, device_id, primary_port, vlan_id):
    self.device_id = device_id
    self.primary_port = primary_port
    self.vlan_id = vlan_id

    self.device = NetworkDevices.get(device_id)
    self.device.add_assignment(self)

  @classmethod
  def parse_row(cls, row):
    return Assignment(int(row['device_id']),
                      (row['primary_port'] is '1'),
                      int(row['vlan_id']))

  @classmethod
  def load(cls, file_name):
    super().load(file_name)
    NetworkDevices.prep_devices()

  def __repr__(self): # FIX ME replace all with __str__
    return "Assignment(device=%d,vlan=%d,primary=%s)" % (self.device_id, self.vlan_id, self.primary_port)

class Reservation(TelnyxBase):
  _data = []

  def __init__(self, request_id, device_id, primary_port, vlan_id):
    self.request_id = request_id
    self.device_id = device_id
    self.primary_port = primary_port
    self.vlan_id = vlan_id

  @classmethod
  def parse_row(cls, row):
    return Reservation(int(row['request_id']),
                      int(row['device_id']),
                      (row['primary_port'] is '1'),
                      int(row['vlan_id']))

  def __repr__(self):
    return "Reservation(request=%d,device=%d,primary=%s,vlan=%d)" % (self.request_id, self.device_id, self.primary_port, self.vlan_id)

  def __eq__(self, other):
    return self.__repr__() == other.__repr__()

class RequestProcessor:
  def __init__(self, assignments):
    self.assignments = assignments
    self.reservations = []

  def run(self, requests):
    for request in requests:
      logger.debug("---- request: %s", request)
      NetworkDevices.dump_devices()
      self.reservations.extend(self.process_request(request))
    return self.reservations

  def process_request(self, request):
    if request.redundant:
      return self.process_redundant_request(request)
    else:
      return self.process_singular_request(request)

  def process_singular_request(self, request):
    lowest_vlans = {}
    for device_id, device in NetworkDevices._devices.items():
      if len(device.singular_assignments_sorted) > 0:
        lowest_vlans[device_id] = device.singular_assignments_sorted[0]
    # logger.debug("lowest_vlans: %s", lowest_vlans)
    device_id, assignment = min(lowest_vlans.items(), key=lambda x: x[1].vlan_id)
    # logger.debug("device: %d assignment: %s", device_id, assignment)
    NetworkDevices.get(device_id).singular_assignments_sorted.remove(assignment)
    reservations = [Reservation(request.request_id, assignment.device_id, True, assignment.vlan_id)]
    return reservations

  def process_redundant_request(self, request):
    lowest_vlans = {}
    for device_id, device in NetworkDevices._devices.items():
      if len(device.redundant_assignments_sorted) > 0:
        lowest_vlans[device_id] = device.redundant_assignments_sorted[0]
    # logger.debug("lowest_vlans: %s", lowest_vlans)
    device_id, assignment = min(lowest_vlans.items(), key=lambda x: x[1].vlan_id)
    # logger.debug("device: %d assignment: %s", device_id, assignment)
    NetworkDevices.get(device_id).redundant_assignments_sorted.remove(assignment)
    reservations = [
      Reservation(request.request_id, assignment.device_id, False, assignment.vlan_id),
      Reservation(request.request_id, assignment.device_id, True, assignment.vlan_id)
    ]
    return reservations
