import argparse

class AppConfig:
    def __init__(self, input_path, output_path, log_level):
        self.input_path = input_path
        self.output_path = output_path
        self.log_level = log_level

    def __repr__(self):
        return (f"AppConfig(input_path={self.input_path}, "
                f"output_path={self.output_path}, "
                f"log_level={self.log_level})")

import argparse

def parse_arguments():
    parser = argparse.ArgumentParser(description='PySpark Application Configuration')

    # Define command-line arguments
    parser.add_argument('--input', type=str, required=True, help='Path to the input data')
    parser.add_argument('--output', type=str, required=True, help='Path to the output data')
    parser.add_argument('--log_level', type=str, default='INFO', help='Logging level (e.g., DEBUG, INFO, WARN, ERROR)')

    args = parser.parse_args()

    # Create an instance of AppConfig with parsed arguments
    config = AppConfig(
        input_path=args.input,
        output_path=args.output,
        log_level=args.log_level
    )

    return config
