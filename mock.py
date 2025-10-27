import pandas as pd
from faker import Faker
import random
from datetime import timedelta

fake = Faker()
Faker.seed(42)

def generate_claims(n=100):
    claims = []
    for _ in range(n):
        service_date = fake.date_between(start_date='-90d', end_date='today')
        claim_date = service_date + timedelta(days=random.randint(0, 5))
        status = random.choice(['Paid', 'Denied', 'Pending'])
        paid = round(random.uniform(50, 2000), 2)
        total = paid + round(random.uniform(0, 500), 2)

        claims.append({
            "claim_id": f"CLM{fake.random_int(100000,999999)}",
            "policy_id": f"POL{fake.random_int(10000,99999)}",
            "member_id": f"MBR{fake.random_int(1000,9999)}",
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=80),
            "provider_name": fake.company() + " Medical Group",
            "provider_npi": fake.random_number(digits=10, fix_len=True),
            "claim_date": claim_date,
            "service_date": service_date,
            "diagnosis_code": random.choice(["E11.9", "I10", "J45.909", "M54.5", "E78.5"]),
            "procedure_code": random.choice(["99213", "93000", "80053", "71020", "85025"]),
            "claim_amount": total,
            "paid_amount": paid if status == "Paid" else 0,
            "claim_status": status,
            "adjudicated_date": claim_date + timedelta(days=random.randint(1, 10)),
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "USA"
        })
    return pd.DataFrame(claims)

# Generate and save
df = generate_claims(200)
df.to_csv("claims_data.csv", index=False)
print("✅ 200 fake claim records written to claims_data.csv")
