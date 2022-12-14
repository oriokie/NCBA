import xml.etree.ElementTree as ET
from glob import iglob
from os.path import isfile, join
from csv import DictWriter
from collections import ChainMap
import pandas as pd
from openpyxl import Workbook
import os


if not os.path.isdir("xml"):
    os.mkdir("xml")

def stripNs(el):
  if el.tag.startswith("{"):
    el.tag = el.tag.split('}', 1)[1]  # strip namespace
  for k in el.attrib.keys():
    if k.startswith("{"):
      k2 = k.split('}', 1)[1]
      el.attrib[k2] = el.attrib[k]
      del el.attrib[k]
  for child in el:
    stripNs(child)


xml_root = "./xml"
pattern = "2022*"
data = ChainMap()
for filename in iglob(join(xml_root, pattern)):
    if isfile(filename):
        tree = ET.parse(filename)
        root = tree.getroot()
        stripNs(root)
        for node in root.iter():
            temp = {node.tag: node.text}
            data = data.new_child(temp)

wb = Workbook(write_only=True)
ws = wb.create_sheet()
ws.append(list(data))  # write header
for row in data.maps[:-1]:
    ws.append([row.get(key, "") for key in data])
wb.save(join(xml_root, "data.xlsx"))
