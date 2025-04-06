import csv
import grpc
import time
from concurrent import futures
import mini2_pb2
import mini2_pb2_grpc

class CollisionDataClient:
    def __init__(self, server_address='localhost:50056'):
        self.server_address = server_address
        # Create gRPC channel
        self.channel = grpc.insecure_channel(server_address)
        # Create stub (client)
        self.stub = mini2_pb2_grpc.EntryPointServiceStub(self.channel)

    def parse_collision_data(self, row):
        """Convert CSV row to CollisionData message"""
        try:
            collision = mini2_pb2.CollisionData(
                crash_date=row['CRASH DATE'],
                crash_time=row['CRASH TIME'],
                borough=row['BOROUGH'],
                zip_code=row['ZIP CODE'],
                number_of_persons_injured=int(row['NUMBER OF PERSONS INJURED']),
                number_of_persons_killed=int(row['NUMBER OF PERSONS KILLED']),
                number_of_pedestrians_injured=int(row['NUMBER OF PEDESTRIANS INJURED']),
                number_of_pedestrians_killed=int(row['NUMBER OF PEDESTRIANS KILLED']),
                number_of_cyclist_injured=int(row['NUMBER OF CYCLIST INJURED']),
                number_of_cyclist_killed=int(row['NUMBER OF CYCLIST KILLED']),
                number_of_motorist_injured=int(row['NUMBER OF MOTORIST INJURED']),
                number_of_motorist_killed=int(row['NUMBER OF MOTORIST KILLED']),
                collision_id=row['COLLISION_ID']
            )
            return collision
        except ValueError as e:
            print(f"Error parsing row: {e}")
            return None

    def stream_data(self, csv_file_path, batch_size=100):
        """Stream data from CSV file to Server A"""
        try:
            with open(csv_file_path, 'r') as file:
                csv_reader = csv.DictReader(file)
                total_records = 0
                
                # Define a generator function to create the request iterator
                def generate_data():
                    nonlocal total_records
                    for row in csv_reader:
                        collision = self.parse_collision_data(row)
                        if collision:
                            yield collision
                            total_records += 1
                            if total_records % batch_size == 0:
                                print(f"Sent {total_records} records...")
                
                # Call the streaming RPC with the request iterator
                response = self.stub.StreamCollisions(generate_data())
                
                print(f"Total records processed: {total_records}")

        except FileNotFoundError:
            print(f"Error: Could not find CSV file at {csv_file_path}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        finally:
            self.channel.close()

def main():
    # Create client instance
    client = CollisionDataClient()
    
    # Path to your CSV file
    csv_file_path = "Motor_Vehicle_Collisions_-_Crashes_20250402.csv"
    
    print("Starting to stream collision data to Server A...")
    start_time = time.time()
    
    # Use a smaller batch size to reduce pressure on the system
    client.stream_data(csv_file_path, batch_size=10)
    
    end_time = time.time()
    print(f"Data streaming completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()
