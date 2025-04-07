import csv
import grpc
import time
from multiprocessing import Process
import mini2_pb2
import mini2_pb2_grpc


class CollisionDataClient:
    def __init__(self, server_address="169.254.181.194:50056"):
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

    def stream_data(self, csv_file_path, start_line, end_line, batch_size=100):
        """Stream data from a specific portion of the CSV file to Server A"""
        try:
            with open(csv_file_path, "r") as file:
                csv_reader = csv.DictReader(file)
                total_records = 0

                # Skip header rows and then skip until start_line
                for _ in range(start_line):
                    next(csv_reader, None)

                # Define a generator that reads until the end_line
                def generate_data():
                    nonlocal total_records
                    # Continue reading until we've processed the designated segment
                    for row in csv_reader:
                        if total_records >= (end_line - start_line):
                            break
                        collision = self.parse_collision_data(row)
                        if collision:
                            total_records += 1
                            yield collision
                            if total_records % batch_size == 0:
                                print(
                                    f"Client starting at line {start_line}: Sent {total_records} records..."
                                )

                response = self.stub.StreamCollisions(generate_data())
                print(
                    f"Client starting at line {start_line}: Total records processed: {total_records}"
                )

        except FileNotFoundError:
            print(f"Error: Could not find CSV file at {csv_file_path}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
        finally:
            self.channel.close()


def count_data_rows(csv_file_path):
    """Count the number of data rows in the CSV file (excluding header)"""
    with open(csv_file_path, "r") as file:
        return sum(1 for _ in file) - 1  # subtract one for header


def client_process(csv_file_path, start_line, end_line, batch_size):
    client = CollisionDataClient()
    client.stream_data(csv_file_path, start_line, end_line, batch_size)


def main():
    csv_file_path = "collisions.csv"  # Path to your CSV file
    total_rows = count_data_rows(csv_file_path)
    print(f"Total data rows in CSV: {total_rows}")

    # Partition the CSV into 3 segments
    segment_size = total_rows // 3
    segments = []
    for i in range(3):
        start = i * segment_size
        # For the last segment, include any remaining rows.
        end = (i + 1) * segment_size if i < 2 else total_rows
        segments.append((start, end))
        print(f"Segment {i + 1}: Lines {start} to {end}")

    batch_size = 100  # Adjust as needed

    start_time = time.time()
    
    # Spawn three client processes
    processes = []
    for start, end in segments:
        p = Process(target=client_process, args=(csv_file_path, start, end, batch_size))
        processes.append(p)
        p.start()

    # Wait for all processes to complete
    for p in processes:
        p.join()

    end_time = time.time()
    print(f"Data streaming completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()