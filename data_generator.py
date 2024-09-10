import sys 
import rapidjson as json # serialize Python objects into JSON format.
import optional_faker as _ # helper lib for 'faker'
import uuid # generates universally unique identifiers
import random # genarates random data, which are used to create random data

from dotenv import load_dotenv
from faker import Faker # generates fake data
from datetime import date, datetime

#function loads environment variables from '.env' file.
load_dotenv()

# create instance and predifine a list of ski resorts.
fake = Faker()
resorts = ["Vail", "Beaver Creek", "Breckenridge", "Keystone", "Crested Butte", "Park City", "Heavenly", "Northstar",
           "Kirkwood", "Whistler Blackcomb", "Perisher", "Falls Creek", "Hotham", "Stowe", "Mount Snow", "Okemo",
           "Hunter Mountain", "Mount Sunapee", "Attitash", "Wildcat", "Crotched", "Stevens Pass", "Liberty", "Roundtop", 
           "Whitetail", "Jack Frost", "Big Boulder", "Alpine Valley", "Boston Mills", "Brandywine", "Mad River",
           "Hidden Valley", "Snow Creek", "Wilmot", "Afton Alps" , "Mt. Brighton", "Paoli Peaks"]    

# This function generates a single lift ticket and prints in JSON format
def print_lift_ticket():
    global resorts, fake
    state = fake.state_abbr() # generates random state abbreiations.
    lift_ticket = {'txid': str(uuid.uuid4()),
                   'rfid': hex(random.getrandbits(96)),
                   'resort': fake.random_element(elements=resorts),
                   'purchase_time': datetime.utcnow().isoformat(),
                   'expiration_time': date(2023, 6, 1).isoformat(),
                   'days': fake.random_int(min=1, max=7),
                   'name': fake.name(),
                   'address': fake.optional({'street_address': fake.street_address(), 
                                             'city': fake.city(), 'state': state, 
                                             'postalcode': fake.postalcode_in_state(state)}),
                   'phone': fake.optional(fake.phone_number()),
                   'email': fake.optional(fake.email()),
                   'emergency_contact' : fake.optional({'name': fake.name(), 'phone': fake.phone_number()}),
    }
    d = json.dumps(lift_ticket) + '\n'
    sys.stdout.write(d)

if __name__ == "__main__":
    args = sys.argv[1:]
    total_count = int(args[0])
    for _ in range(total_count):
        print_lift_ticket()
    print('')
