import sys
workload = sys.argv[1]
processing_results_file = sys.argv[2]
real_results_file = sys.argv[3]

SKIP_NUM = 15
cur_skip_real = 0
cur_skip_results = 0
last_one = ""

if workload == "ysb":
    real_results_dict = {}
    with open(processing_results) as real_results_file:
        print("real results")
        for row in real_results_file:
            if cur_skip_real < SKIP_NUM:
                cur_skip_real = cur_skip_real + 1
                continue
            row_values = row.split(',')
            print(row_values[0])
            print(row_values[1])
            real_results_dict[row_values[0]] = int(row_values[1])


    with_fake_data = {}
    without_fake_data = {}
    with open(real_results) as flink_results:
        print("flink results")
        for row in flink_results:
            if cur_skip_results < SKIP_NUM:
                cur_skip_results = cur_skip_results + 1
                continue
            row_values = row.split(',')
            print(row_values)
            with_fake_data[row_values[0]] = int(row_values[1])
            without_fake_data[row_values[0]] = int(row_values[2][:-2])
            last_one = row_values[0]

    del with_fake_data[last_one]
    del without_fake_data[last_one]

    errors_with_fake = []
    errors_without_fake = []
    real_keys = real_results_dict.keys()
    results_keys = with_fake_data.keys()

    for k in list(set(real_keys) & set(results_keys)):
        errors_with_fake.append(abs(real_results_dict[k] - with_fake_data[k])/real_results_dict[k])
        errors_without_fake.append(abs(real_results_dict[k] - without_fake_data[k])/real_results_dict[k])

    print("errors_with_fake")
    print(errors_with_fake)
    print(sum(errors_with_fake)/len(errors_with_fake))
    print("errors_without_fake")
    print(errors_without_fake)
    print(sum(errors_without_fake)/len(errors_without_fake))
elif workload == "nyt":
    real_results_dict = {}
    with open(real_results_file) as real_results_file:
        print("real results")
        for row in real_results_file:
            if cur_skip_real < SKIP_NUM:
                cur_skip_real = cur_skip_real + 1
                continue
            row_values = row.split(',')
            print(row_values)
            real_results_dict[row_values[0]] = float(row_values[1])

    with_fake_data = {}
    without_fake_data = {}
    with open(processing_results_file) as flink_results:
        print("flink results")
        for row in flink_results:
            if cur_skip_results < SKIP_NUM:
                cur_skip_results = cur_skip_results + 1
                continue
            row_values = row.split(',')
            print(row_values)
            with_fake_data[row_values[0]] = float(row_values[1])
            without_fake_data[row_values[0]] = float(row_values[2])
            last_one = row_values[0]

    del with_fake_data[last_one]
    del without_fake_data[last_one]
    errors_with_fake = []
    errors_without_fake = []
    real_keys = real_results_dict.keys()
    results_keys = with_fake_data.keys()

    for k in list(set(real_keys) & set(results_keys)):
        errors_with_fake.append(abs(real_results_dict[k] - with_fake_data[k])/real_results_dict[k])
        errors_without_fake.append(abs(real_results_dict[k] - without_fake_data[k])/real_results_dict[k])

    print("errors_with_fake")
    print(errors_with_fake)
    print(sum(errors_with_fake)/len(errors_with_fake))
    print("errors_without_fake")
    print(errors_without_fake)
    print(sum(errors_without_fake)/len(errors_without_fake))
