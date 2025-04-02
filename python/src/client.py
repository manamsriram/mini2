import csv
import grpc
import time
from concurrent import futures
import mini2_pb2
import mini2_pb2_grpc


class CollisionDataClient:
    def __init__(self, server_address="localhost:50051"):
        self.server_address = server_address
        # Create gRPC channel
        self.channel = grpc.insecure_channel(server_address)
        # Create stub (client)
        self.stub = mini2_pb2_grpc.EntryPointServiceStub(self.channel)

    def parse_collision_data(self, row):
        """Convert CSV row to CollisionData message"""
        try:
            collision = mini2_pb2.CollisionData(
                crash_date=row["CRASH DATE"],
                crash_time=row["CRASH TIME"],
                borough=row["BOROUGH"],
                zip_code=row["ZIP CODE"],
                latitude=float(row["LATITUDE"]) if row["LATITUDE"] else 0.0,
                longitude=float(row["LONGITUDE"]) if row["LONGITUDE"] else 0.0,
                location=row["LOCATION"],
                on_street_name=row["ON STREET NAME"],
                cross_street_name=row["CROSS STREET NAME"],
                off_street_name=row["OFF STREET NAME"],
                number_of_persons_injured=int(row["NUMBER OF PERSONS INJURED"]),
                number_of_persons_killed=int(row["NUMBER OF PERSONS KILLED"]),
                number_of_pedestrians_injured=int(row["NUMBER OF PEDESTRIANS INJURED"]),
                number_of_pedestrians_killed=int(row["NUMBER OF PEDESTRIANS KILLED"]),
                number_of_cyclist_injured=int(row["NUMBER OF CYCLIST INJURED"]),
                number_of_cyclist_killed=int(row["NUMBER OF CYCLIST KILLED"]),
                number_of_motorist_injured=int(row["NUMBER OF MOTORIST INJURED"]),
                number_of_motorist_killed=int(row["NUMBER OF MOTORIST KILLED"]),
                collision_id=row["COLLISION_ID"],
            )
            return collision
        except ValueError as e:
            print(f"Error parsing row: {e}")
            return None

    def generate_collisions_from_csv(self, csv_file_path):
        """
        Generator function that reads CSV rows and yields CollisionData messages one at a time.
        """
        try:
            with open(csv_file_path, "r") as file:
                csv_reader = csv.DictReader(file)
                for row in csv_reader:
                    collision = self.parse_collision_data(row)
                    if collision:
                        yield collision
        except FileNotFoundError:
            print(f"Error: Could not find CSV file at {csv_file_path}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    def stream_data(self, csv_file_path):
        """
        Stream data to the EntryPointService's StreamCollisions method.
        """
        collision_generator = self.generate_collisions_from_csv(csv_file_path)
        empty_response = self.stub.StreamCollisions(collision_generator)
        # The server returns an Empty message, so there's nothing special in it
        print("Streaming completed successfully.")

        # Once we're done, close the channel
        self.channel.close()


def main():
    client = CollisionDataClient(server_address="localhost:50051")
    csv_file_path = "Motor_Vehicle_Collisions_-_Crashes_20250223.csv"

    print("Starting to stream collision data to Server A (via StreamCollisions)...")
    start_time = time.time()

    client.stream_data(csv_file_path)

    end_time = time.time()
    print(f"Data streaming completed in {end_time - start_time:.2f} seconds")


if __name__ == "__main__":
    main()
