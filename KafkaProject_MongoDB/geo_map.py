import math
import pandas as pd


class GEO_Map():
	"""
	It hold the  map for zip code and its latitute and longitute
	"""
	__instance = None

	@staticmethod
	def get_instance():
		""" Static access method. """
		if GEO_Map.__instance == None:
			GEO_Map()
		return GEO_Map.__instance

	def __init__(self):
		""" Virtually private constructor. """
		if GEO_Map.__instance != None:
			raise Exception("This class is a singleton!")
		else:
			GEO_Map.__instance = self
			self.map = pd.read_csv("Capstone Project 1 - Credit Card Fraud Detection/KafkaProject_MongoDB/uszipsv.csv", header=None, names=['A',"B",'C','D','E'])
			self.map['A'] =  self.map['A'].astype(str)

	def get_lat(self, pos_id):
		return self.map[self.map.A == pos_id ].B

	def get_long(self, pos_id):
		return self.map[self.map.A == pos_id ].C

	def distance(self, lat1, long1, lat2, long2):
		theta = long1 - long2
		dist = math.sin(self.deg2rad(lat1)) * math.sin(self.deg2rad(lat2)) + math.cos(self.deg2rad(lat1)) * math.cos(self.deg2rad(lat2)) * math.cos(self.deg2rad(theta))
		dist = math.acos(dist)
		dist = self.rad2deg(dist)
		dist = dist * 60 * 1.1515 * 1.609344
		return dist

	def rad2deg(self, rad):
		return rad * 180.0 / math.pi

	def deg2rad(self, deg):
		return deg * math.pi / 180.0









