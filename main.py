from pyspark.sql import SparkSession
from QueryLoader import query_loader

# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    df = query_loader()
    df.data(1).show()


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    print_hi('PyCharm')
    for i in range(8):
        print(i)



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
