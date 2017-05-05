"""Config file with global variables"""

# List of required fields
REQUIRED_FIELDS = ('playerID', 'nameFirst', 'nameLast', 'weight', 'position')

# Field for resolving table name for persisting
TABLE_RESOLVER_FIELD = 'position'

# Field with entity id
ID_FIELD = 'playerID'

# List of required fields that can be updated
UPDATABLE_FIELDS = ('weight',)

# Default input file
INPUT_FILE = 'players.csv'
