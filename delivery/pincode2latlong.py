file_content = open("/home/marino/Desktop/IN.txt").read()
states = set()

def getData(from_state, exclude_states):	
	mapper = {}
	for row in file_content.split("\n"):
		try:
			c, pincode, name, state, a, b, c, d, e, lat, loc_long, f = row.split("\t")
			states.add(state)
			if len(exclude_states) != 0 and state not in exclude_states:
				mapper[pincode] = {"lat": float(lat), "long": float(loc_long)}
			elif len(exclude_states) == 0 and (from_state == None or state == from_state):
				mapper[pincode] = {"lat": float(lat), "long": float(loc_long)}
		except:
			pass
	return mapper


def findLatLong(pincodes, state, exclude_states):
	mapper = getData(state, exclude_states)
	arr = []
	for pincode in pincodes:
		if pincode in mapper:
			v = mapper[pincode]
			tmp = (v["lat"], v["long"])
			arr.append(tmp)
	return arr

# print findLatLong(["560054"], "Karnataka")

