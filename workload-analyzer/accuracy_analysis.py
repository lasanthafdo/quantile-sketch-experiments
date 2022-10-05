import sys

import matplotlib.pyplot as plt
import matplotlib as mpl
import numpy as np
import pandas as pd
import seaborn as sns
from matplotlib.ticker import AutoLocator
import matplotlib.style as style

style.use('tableau-colorblind10')


def produce_bar_plot(mid_q_dict, upper_q_dict, plot_title, x_label, y_label, ylim_top, plot_filename):
    plot_data = [
        ["Mid", mid_q_dict['moments'], mid_q_dict['dds'], mid_q_dict['udds'], mid_q_dict['kll'], mid_q_dict['req']],
        ["Upper", upper_q_dict['moments'], upper_q_dict['dds'], upper_q_dict['udds'], upper_q_dict['kll'],
         upper_q_dict['req']]]
    df = pd.DataFrame(plot_data, columns=["Quantile range", "Moments", "DDS", "UDDS", "KLL", "REQ"])
    ax = df.plot(x="Quantile range", y=["Moments", "DDS", "UDDS", "KLL", "REQ"], kind="bar")
    for p in ax.patches:
        if p.get_height() > ylim_top:
            ax.annotate(str(p.get_height()), (p.get_x() + 0.03, 0.04), rotation=90)

    plt.ylim(0, ylim_top)
    plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    # plt.legend(loc="lower right")
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots for " + plot_title)


def produce_bar_plot_wt_err_bars(mid_q_dict, upper_q_dict, p99_q_dict, plot_title, x_label, y_label, ylim_top,
                                 plot_filename):
    plot_data = [
        ["Mid", mid_q_dict['moments'], mid_q_dict['dds'], mid_q_dict['udds'], mid_q_dict['kll'], mid_q_dict['req']],
        ["Upper", upper_q_dict['moments'], upper_q_dict['dds'], upper_q_dict['udds'], upper_q_dict['kll'],
         upper_q_dict['req']],
        ["0.99", p99_q_dict['moments'], p99_q_dict['dds'], p99_q_dict['udds'], p99_q_dict['kll'], p99_q_dict['req']]]
    order = ["Mid", "Upper", "0.99"]
    data_df = pd.DataFrame(plot_data, columns=["Quantile range", "Moments", "DDS", "UDDS", "KLL", "REQ"])
    data_df = data_df.explode(list(["Moments", "DDS", "UDDS", "KLL", "REQ"]))
    # data_df =  data_df.set_index("Quantile range").loc[order]
    mean_data_df = data_df.groupby('Quantile range').mean().round(4)
    mean_data_df = mean_data_df.reindex(order).reset_index()
    x_ci = data_df.groupby('Quantile range').agg(
        lambda x: np.sqrt(x.pow(2).mean() - pow(x.mean(), 2)) * 1.96 / np.sqrt(x.size))
    x_ci = x_ci.reindex(order).reset_index()
    print("=========== CI values ============ ")
    print(x_ci)
    print("=========== Avg. Acc ============ ")
    print(mean_data_df)
    fig, ax = plt.subplots(figsize=(4, 3))
    print(mean_data_df)
    mean_data_df.plot(x="Quantile range", y=["Moments", "DDS", "UDDS", "KLL", "REQ"],
                      style=['o-', 'v-', '^-', '|--', 'x--'], kind="bar", ax=ax)  # , "x", "o", "O", ".", "*" ])
    for i, alg in enumerate(["Moments", "DDS", "UDDS", "KLL", "REQ"]):
        offset = -0.2 + i * 0.1
        ax.errorbar(mean_data_df.index + offset, mean_data_df[alg], yerr=x_ci[alg], ecolor='k', capsize=3,
                    linestyle="None")
    bars = ax.patches

    hatches = ["....", "....", "....", "\\\\\\\\", "\\\\\\\\", "\\\\\\\\", "////", "////", "////", "", "",
               "", "xxxx", "xxxx", "xxxx"]
    # hatches = ["/", "/", "\\", "\\", "|", "|", "-", "-", "+", "+"]
    alg_dict = {0: "Moments", 1: "DDS", 2: "UDDS", 3: "KLL", 4: "REQ"}
    for i, (bar, hatch) in enumerate(zip(bars, hatches)):
        bar.set_hatch(hatch)
        if bar.get_height() > 0.05:
            err_bar = round(x_ci.iloc[i % 3][alg_dict[i // 3]], 4)
            ax.annotate(str(bar.get_height()) + "(" + str(err_bar) + ")", (bar.get_x() + 0.01, 0.01), rotation=90,
                        fontsize=7)

    # plt.errorbar('Quantile range', ["Moments", "DDS", "UDDS", "KLL", "REQ"], yerr=x_ci, data=mean_data_df, linestyle="None", capsize=3)

    plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    # plt.title(plot_title)
    if plot_title == 'Power':
        ax.legend(loc="upper center", bbox_to_anchor=(0.67, 1))
    elif plot_title == 'Pareto':
        ax.legend(loc="upper left")
    else:
        ax.legend()
    fig.tight_layout()
    ax.autoscale(enable=True)
    plt.ylim(0, ylim_top)
    plt.savefig(plot_filename)
    plt.clf()
    print("Finished generating plots.")


def calc_accuracy(approx_df, real_df):
    acc_calc_df = pd.merge(approx_df, real_df, on='window_id', suffixes=("_approx", "_real"))
    acc_calc_df['pct_01_acc'] = abs(
        acc_calc_df.pct_01_approx - acc_calc_df.pct_01_real) / acc_calc_df.pct_01_real
    acc_calc_df['pct_05_acc'] = abs(
        acc_calc_df.pct_05_approx - acc_calc_df.pct_05_real) / acc_calc_df.pct_05_real
    acc_calc_df['pct_25_acc'] = abs(
        acc_calc_df.pct_25_approx - acc_calc_df.pct_25_real) / acc_calc_df.pct_25_real
    acc_calc_df['pct_50_acc'] = abs(
        acc_calc_df.pct_50_approx - acc_calc_df.pct_50_real) / acc_calc_df.pct_50_real
    acc_calc_df['pct_75_acc'] = abs(
        acc_calc_df.pct_75_approx - acc_calc_df.pct_75_real) / acc_calc_df.pct_75_real
    acc_calc_df['pct_90_acc'] = abs(
        acc_calc_df.pct_90_approx - acc_calc_df.pct_90_real) / acc_calc_df.pct_90_real
    acc_calc_df['pct_95_acc'] = abs(
        acc_calc_df.pct_95_approx - acc_calc_df.pct_95_real) / acc_calc_df.pct_95_real
    acc_calc_df['pct_98_acc'] = abs(
        acc_calc_df.pct_98_approx - acc_calc_df.pct_98_real) / acc_calc_df.pct_98_real
    acc_calc_df['pct_99_acc'] = abs(
        acc_calc_df.pct_99_approx - acc_calc_df.pct_99_real) / acc_calc_df.pct_99_real
    acc_calc_df = acc_calc_df[
        ['run_id', 'window_id', 'pct_01_acc', 'pct_05_acc', 'pct_25_acc', 'pct_50_acc', 'pct_75_acc',
         'pct_90_acc', 'pct_95_acc', 'pct_98_acc', 'pct_99_acc', 'query_time', 'win_size']]
    acc_calc_df = acc_calc_df.groupby('run_id', as_index=False)[
        ['pct_01_acc', 'pct_05_acc', 'pct_25_acc', 'pct_50_acc', 'pct_75_acc',
         'pct_90_acc', 'pct_95_acc', 'pct_98_acc', 'pct_99_acc', 'query_time', 'win_size']].mean()
    mid_q_all_accs = np.array([acc_calc_df['pct_05_acc'], acc_calc_df['pct_25_acc'],
                               acc_calc_df['pct_50_acc'], acc_calc_df['pct_75_acc'],
                               acc_calc_df['pct_90_acc']]).mean(axis=0)
    upper_q_all_accs = np.array([acc_calc_df['pct_95_acc'], acc_calc_df['pct_98_acc']]).mean(axis=0)
    point_99_q_all_accs = np.array([acc_calc_df['pct_99_acc']]).mean(axis=0)
    mid_quantile_accs = [acc_calc_df['pct_05_acc'].mean(), acc_calc_df['pct_25_acc'].mean(),
                         acc_calc_df['pct_50_acc'].mean(), acc_calc_df['pct_75_acc'].mean(),
                         acc_calc_df['pct_90_acc'].mean()]
    upper_quantile_accs = [acc_calc_df['pct_95_acc'].mean(), acc_calc_df['pct_98_acc'].mean()]
    point_99_quantile_accs = [acc_calc_df['pct_99_acc'].mean()]
    return acc_calc_df, mid_quantile_accs, upper_quantile_accs, point_99_quantile_accs, mid_q_all_accs, upper_q_all_accs, point_99_q_all_accs


def plot_hist_dataset(data_df, col_of_interest, x_label, y_label, plot_title, plot_file_name):
    data = data_df[col_of_interest]
    fig, ax = plt.subplots(figsize=(4, 3))
    if plot_title == 'Uniform':
        sns.histplot(data=data, stat='probability', ax=ax, color='k', binwidth=1)
    elif plot_title == 'Adaptability PDF':
        sns.histplot(data=data, stat='probability', ax=ax, color='k', binwidth=1)
    else:
        sns.histplot(data=data, stat='probability', ax=ax, color='k')
    bins = np.histogram_bin_edges(data_df[col_of_interest], bins='auto')
    print(bins)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    fig.tight_layout()
    ax.autoscale(enable=True)
    if plot_title == 'NYT':
        ax.set_xlim(0, 100)
    # plt.show()
    plt.savefig(plot_file_name)
    plt.clf()
    print("Finished generating PDF plot for " + plot_title)


def plot_hist_bins_dataset(data_df, col_of_interest, x_label, y_label, plot_title, plot_file_name):
    tot_count = len(data_df.index)
    data_df = data_df.round()
    data_df = data_df.groupby('sampled_val')["index"].count().reset_index(name="count")
    data_df['count'] = data_df['count'] / tot_count
    print(data_df)
    # sns.histplot(data=data, stat='probability', ax=ax, binwidth=1, log_scale=(False,True))
    ax = data_df.plot(x='sampled_val', y='count', kind='bar')
    ax.get_legend().remove()
    ax.xaxis.set_major_locator(AutoLocator())
    plt.xticks(rotation=0)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    # plt.ylim(0,0.01)
    if plot_title == 'NYT':
        plt.xlim(0, 100)
    elif plot_title == 'Pareto':
        plt.yscale('log')
        # plt.xscale('log')
        plt.xlim(1, 10000)
    # plt.savefig(plot_file_name)
    plt.show()
    plt.clf()
    print("Finished generating PDF plot for " + plot_title)


def plot_hist_pareto_dataset(data_df, col_of_interest, x_label, y_label, plot_title, plot_file_name):
    data = data_df[col_of_interest]
    fig, ax = plt.subplots(figsize=(4, 3))
    p = sns.histplot(data=data, stat='probability', ax=ax, bins=10000, log_scale=(False, True), color='k')
    # bins = np.histogram_bin_edges(data_df[col_of_interest], bins='fd')
    ax.set_xlim(right=6000000)
    # ax.set_ylim(top=1e-3)
    # ax.ticklabel_format(useOffset=False, style='plain', axis='x')
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.title(plot_title)
    if plot_title == 'NYT':
        ax.set_xlim(0, 100)
    fig.tight_layout()
    ax.autoscale(enable=True)
    plt.savefig(plot_file_name)
    plt.clf()
    print("Finished generating PDF plot for " + plot_title)


if __name__ == '__main__':
    report_folder = sys.argv[1]
    datasets = ['pareto', 'uniform', 'power', 'nyt']
    ds_to_plot = ['pareto', 'uniform', 'power', 'nyt', 'adaptability']
    ds_label_names = {'pareto': 'Pareto', 'uniform': 'Uniform', 'nyt': 'NYT', 'power': 'Power'}
    num_windows = 10
    pd.set_option('display.max_columns', 12)
    mpl.rcParams['hatch.linewidth'] = 0.5
    data_set_analysis = False
    calc_missing_pct = False
    display_ci = True
    verbose = True

    if data_set_analysis:
        ds_file_ext = '.pdf'
        nrows_read = 1000000
        if 'nyt' in ds_to_plot:
            nyt_file = '/home/m34ferna/flink-benchmarks/nyt-data.csv'
            nyt_names = ['medallion', 'hack_license', 'vendor_id', 'pickup_datetime', 'payment_type', 'fare_amount',
                         'surcharge', 'mta_tax', 'tip_amount', 'tolls_amount', 'total_amount']
            nyt_ds = pd.read_csv(nyt_file, header=None, names=nyt_names, nrows=nrows_read)
            plot_file_name_nyt = report_folder + '/plots/nyt_dist_' + str(nrows_read) + ds_file_ext
            plot_hist_dataset(nyt_ds, 'total_amount', 'Value', 'Probability', 'NYT', plot_file_name_nyt)

        if 'power' in ds_to_plot:
            power_file = '/home/m34ferna/flink-benchmarks/household_power_consumption.txt'
            power_names = ["Date", "Time", "Global_active_power", "Global_reactive_power", "Voltage",
                           "Global_intensity",
                           "Sub_metering_1", "Sub_metering_2", "Sub_metering_3"]
            power_ds = pd.read_csv(power_file, header=None, names=power_names, nrows=nrows_read, sep=';')
            plot_file_name_pow = report_folder + '/plots/power_dist_' + str(nrows_read) + ds_file_ext
            plot_hist_dataset(power_ds, 'Global_active_power', 'Value', 'Probability', 'Power', plot_file_name_pow)

        if 'pareto' in ds_to_plot:
            pareto_file = '/home/m34ferna/flink-benchmarks/pareto_sample_1000000.csv'
            pareto_names = ["index", "sampled_val"]
            pareto_ds = pd.read_csv(pareto_file, skiprows=1, header=None, names=pareto_names, nrows=nrows_read)
            print(pareto_ds['sampled_val'].max())
            plot_file_name_pto = report_folder + '/plots/pareto_dist_' + str(nrows_read) + ds_file_ext
            plot_hist_pareto_dataset(pareto_ds, 'sampled_val', 'Value', 'Probability', 'Pareto', plot_file_name_pto)

        if 'uniform' in ds_to_plot:
            uniform_file = '/home/m34ferna/flink-benchmarks/uniform_sample_1000000.csv'
            uniform_names = ["index", "sampled_val"]
            uniform_ds = pd.read_csv(uniform_file, skiprows=1, header=None, names=uniform_names, nrows=nrows_read)
            plot_file_name_uni = report_folder + '/plots/uniform_dist_' + str(nrows_read) + ds_file_ext
            plot_hist_dataset(uniform_ds, 'sampled_val', 'Value', 'Probability', 'Uniform', plot_file_name_uni)

        if 'adaptability' in ds_to_plot:
            adapt_file = '/home/m34ferna/flink-benchmarks/adaptability_dist.csv'
            adapt_names = ["index", "adapt_val"]
            adapt_ds = pd.read_csv(adapt_file, skiprows=1, header=None, names=adapt_names, nrows=2000000)
            plot_file_name_adapt = report_folder + '/plots/adaptability_pdf' + ds_file_ext
            plot_hist_dataset(adapt_ds, 'adapt_val', 'Value', 'Probability', 'Adaptability PDF', plot_file_name_adapt)
        # top_k = nyt_ds['total_amount'].value_counts()[:10]
        # print(top_k)
        # print(top_k.sum())
        # # nyt_ds = nyt_ds[(nyt_ds['total_amount'] >= 3.5) & (nyt_ds['total_amount'] <= 12.0)]
        # nyt_ds = nyt_ds.groupby('total_amount', as_index=False).count().drop(
        #     ['medallion', 'hack_license', 'vendor_id', 'pickup_datetime', 'payment_type', 'fare_amount', 'surcharge',
        #      'mta_tax', 'tip_amount'], axis=1)
        # nyt_ds = nyt_ds[nyt_ds['tolls_amount'] >= 2000]
        # print(nyt_ds.tail(50))
        exit(0)

    missing_pcts = []
    ddsc_upper_diff_pcts = []
    ddsc_mid_diff_pcts = []
    ddsc_upper_diff = []
    ddsc_mid_diff = []
    for dataset in datasets:
        algos = ['moments', 'dds', 'ddsc', 'kll', 'req', 'udds']
        real_names = ['window_id', 'pct_01', 'pct_05', 'pct_25', 'pct_50', 'pct_75', 'pct_90', 'pct_95', 'pct_98',
                      'pct_99',
                      'win_size', 'slack_events']
        sketch_names = ['window_id', 'pct_01', 'pct_05', 'pct_25', 'pct_50', 'pct_75', 'pct_90', 'pct_95', 'pct_98',
                        'pct_99', 'query_time']
        sketch_names_kll_req = ['window_id', 'pct_01', 'pct_05', 'pct_25', 'pct_50', 'pct_75', 'pct_90', 'pct_95',
                                'pct_98',
                                'pct_99', 'query_time', 'num_retained']
        mid_q_dict = {}
        upper_q_dict = {}
        p99_q_dict = {}
        mid_q_all_dict = {}
        upper_q_all_dict = {}
        p99_q_all_dict = {}
        algo_ds_acc_dict = {}
        for algo in algos:
            print("Algorithm: " + algo)
            f_algo_ds_real = report_folder + '/' + algo + '-' + dataset + '-real-cleaned.csv'
            f_algo_ds_sketch = report_folder + '/' + algo + '-' + dataset + '-sketch-cleaned.csv'
            algo_ds_real = pd.read_csv(f_algo_ds_real, header=None, names=real_names)
            if calc_missing_pct:
                algo_ds_real['missing_pct'] = algo_ds_real['slack_events'] / algo_ds_real['win_size'] * 100.0
                print(algo_ds_real['missing_pct'])
                missing_pcts.append(np.mean(algo_ds_real[algo_ds_real['missing_pct'] > 0]['missing_pct']))
                print(missing_pcts)

            if algo == 'kll' or algo == 'req':
                algo_ds_sketch = pd.read_csv(f_algo_ds_sketch, header=None, names=sketch_names_kll_req)
            else:
                algo_ds_sketch = pd.read_csv(f_algo_ds_sketch, header=None, names=sketch_names)

            algo_ds_sketch['run_id'] = algo_ds_sketch['window_id'].astype(str).str[:4]
            algo_ds_sketch = algo_ds_sketch.groupby('run_id', as_index=False).apply(
                lambda x: x.iloc[:-1] if len(x) > num_windows else x)

            algo_ds_accuracy, mid_q_accuracy, upper_q_accuracy, point99_q_accuracy, mid_q_all, upper_q_all, point99_q_all = calc_accuracy(
                algo_ds_sketch, algo_ds_real)
            if verbose:
                print(algo_ds_accuracy)
            mid_q_dict[algo] = np.round(np.mean(mid_q_accuracy), 4)
            upper_q_dict[algo] = np.round(np.mean(upper_q_accuracy), 4)
            p99_q_dict[algo] = np.round(np.mean(point99_q_accuracy), 4)
            mid_q_all_dict[algo] = mid_q_all
            upper_q_all_dict[algo] = upper_q_all
            p99_q_all_dict[algo] = point99_q_all
            algo_ds_acc_dict[algo] = algo_ds_accuracy.drop(
                ['run_id', 'query_time', 'win_size', 'pct_01_acc'], axis=1).mean(axis=1)
            print("==================================================")

        if verbose:
            print(">>>>>>>>>>>>> Mid quantile accuracy for " + dataset)
            print(mid_q_all_dict)
            print(">>>>>>>>>>>>> Upper quantile accuracy for " + dataset)
            print(upper_q_all_dict)
            print(">>>>>>>>>>>>> 0.99 quantile accuracy for " + dataset)
            print(p99_q_all_dict)
        ddsc_mid_diff.append(round(abs(mid_q_dict["dds"] - mid_q_dict["ddsc"]), 4))
        ddsc_upper_diff.append(round(abs(upper_q_dict["dds"] - upper_q_dict["ddsc"]), 4))
        ddsc_upper_diff_pcts.append(
            round(abs(upper_q_dict["dds"] - upper_q_dict["ddsc"]) * 100.0 / upper_q_dict["dds"], 4))
        ddsc_mid_diff_pcts.append(round(abs(mid_q_dict["dds"] - mid_q_dict["ddsc"]) * 100.0 / mid_q_dict["dds"], 4))
        file_ext = '.pdf'
        file_name_suffix = '_avg_errors'
        if display_ci:
            file_name_suffix = file_name_suffix + '_ci'
        plot_file_path = report_folder + '/plots/' + dataset + file_name_suffix + file_ext
        y_axis_top = 0.05
        if display_ci:
            produce_bar_plot_wt_err_bars(mid_q_all_dict, upper_q_all_dict, p99_q_all_dict, ds_label_names[dataset],
                                         'Quantiles',
                                         'Avg. Relative Error', y_axis_top, plot_file_path)
            # print('hello')
        else:
            produce_bar_plot(mid_q_dict, upper_q_dict, ds_label_names[dataset], 'Quantiles',
                             'Avg. Relative Error', y_axis_top, plot_file_path)
        for algo, acc_vals in algo_ds_acc_dict.items():
            print(str(np.round(np.mean(acc_vals), 6)))
        input("Press Enter to continue...")

    if calc_missing_pct:
        print(missing_pcts)
        print(np.mean(missing_pcts))
    print("DDSC - Upper diff pcts:" + str(ddsc_upper_diff_pcts))
    print("DDSC - Mid diff pcts:" + str(ddsc_mid_diff_pcts))
    print("DDSC - Upper diffs:" + str(ddsc_upper_diff))
    print("DDSC - Mid diffs:" + str(ddsc_mid_diff))
