import os
from collections import defaultdict

# Define the Map function
def map_phase(file_path):
    word_counts = defaultdict(int)
    with open(file_path, 'r') as file:
        for line in file:
            words = line.strip().split()
            for word in words:
                word_counts[word.lower()] += 1
    return word_counts

# Define the Reduce function
def reduce_phase(mapped_data):
    final_counts = defaultdict(int)
    for word_counts in mapped_data:
        for word, count in word_counts.items():
            final_counts[word] += count
    return final_counts

# Simulate the MapReduce workflow
def map_reduce(directory):
    # Map Phase: Process each file
    mapped_data = []
    for file_name in os.listdir(directory):
        file_path = os.path.join(directory, file_name)
        mapped_data.append(map_phase(file_path))
    
    # Reduce Phase: Aggregate results
    final_result = reduce_phase(mapped_data)
    
    # Sort and display the top 10 words
    sorted_result = sorted(final_result.items(), key=lambda x: x[1], reverse=True)
    print("Top 10 Words:")
    for word, count in sorted_result[:10]:
        print(f"{word}: {count}")

# Run the MapReduce workflow
if __name__ == "__main__":
    data_directory = "data"
    map_reduce(data_directory)
