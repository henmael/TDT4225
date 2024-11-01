from part2.task1 import task1_main
from part2.task2 import task2_main
from part2.task3 import task3_main
from part2.task4 import task4_main
from part2.task5 import task5_main
from part2.task6 import task6_main
from part2.task7 import task7_main
from part2.task8 import task8_main
from part2.task9 import task9_main
from part2.task10 import task10_main
from part2.task11 import task11_main

import os


if __name__ == '__main__':
    with open('.env') as f:
        for line in f:
            name, value = line.strip().split('=', 1)
            os.environ[name] = value
    task1_main()
    task2_main()
    task3_main()
    task4_main()
    task5_main()
    task6_main()
    task7_main()
    task8_main()
    task9_main()
    task10_main()
    task11_main()

