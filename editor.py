class Command(object):
	line = 0
	instruction = ""
	params = []
	def __init__(self, line, instruction, params):
		self.line = line
		self.instruction = instruction
		self.params = params

	def print(self):
		paramString = " ".join(self.params)
		return str(self.line) + " " + self.instruction + " " + paramString

class Editor(object):
	updates = {}
	
	def __init__(self, file_location):
		self.file_location = file_location

	def open(self, operation_mode):
		"""Open the file"""
		self.file = open(self.file_location, mode = operation_mode, encoding = 'utf-8')

	def readFile(self):
		for i, line in enumerate(self.file):
			print(line)

	def command(self, sequence_number, command: Command):
		self.updates[sequence_number] = command

	def append(self):
		self.file.write("\n".join(map(lambda x: x.print(), self.updates.values())))

	def close(self):
		self.file.close()

# Test part starts here

fileName = "commands.txt"
def test_editor():
	editor = Editor(fileName)
	add = Command(0, "ADD", ["this is first line"])
	add1 = Command(1, "ADD", ["this is second line"])
	add2 = Command(2, "ADD", ["this is third line"])
	modify = Command(0, "MOD", ["2", "modified"])
	delete = Command(2, "DEL", [])
	try:
		editor.open('w+')
		# commands are supposed to be given one at a time
		editor.command(0, add)
		editor.command(1, add1)
		editor.command(2, add2)
		editor.command(3, modify)
		editor.command(4, delete)
		# ends commands

		editor.append()	
		editor.readFile()
	finally:
		editor.close()

test_editor()