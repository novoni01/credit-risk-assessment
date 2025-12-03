import cra_functions
import time

def testing_all_functions():
    start = time.time()
    print("testing data path creation")
    data_path = cra_functions.initialize_data_path()
    end = time.time()
    print(f"Time elapsed for initialize_data path:{end-start:.2f} seconds")

    print("getting kaggle datasets")
    start = time.time()
    cra_functions.get_kaggle_data(data_path)
    end = time.time()
    print(f"Time elapsed for get_kaggle_data:{end-start:.2f} seconds")

    try:
        print("turning all csv into dataframes")
        start = time.time()
        dataframe_list = cra_functions.retrieve_training_csv(data_path)
        end = time.time()
        print(f"Time elapsed for retrieve_training_csv:{end-start:.2f} seconds")

    except Exception as e:
        print(f"Error when retrieving all csv's as dataframes: {e}")
    
    try:
        print("getting accepted loans csv")
        start = time.time()
        accepted_df = cra_functions.accepted_loans_df(dataframe_list)
        end = time.time()
        print(f"Time elapsed for accepted_loans_df:{end-start:.2f} seconds")
        print(f"length of sample accepted: {len(accepted_df)}")
    except Exception as e:
        print(f"Error when running accepted_loans_df: {e}")

    try:
        print("getting rejected loans csv")
        start = time.time()
        rejected_df = cra_functions.rejected_loans_df(dataframe_list)
        end = time.time()
        print(f"Time elapsed for rejected_loans_df:{end-start:.2f} seconds")
        print(f"length of rejected: {len(rejected_df)}")
    except Exception as e:
        print(f"Error when getting rejected_loans_df: {e}")


    print("deleting large files")
    start = time.time()
    cra_functions.delete_large_files(data_path)
    end = time.time()
    print(f"Time elapsed for delete_large_files:{end-start:.2f} seconds")

    print("success")



if __name__ == "__main__":
    print("testing functions")
    testing_all_functions()