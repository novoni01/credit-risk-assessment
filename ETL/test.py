import data_ingestion_kaggle
import time

def testing_all_functions():
    start = time.time()
    print("testing json moving")
    data_path = data_ingestion_kaggle.move_kaggle_json()
    end = time.time()
    print(f"Time elapsed for move_kaggle_json path:{end-start:.2f} seconds")

    """"""
    start = time.time()
    print("testing data path creation")
    data_path = data_ingestion_kaggle.initialize_data_path()
    end = time.time()
    print(f"Time elapsed for initialize_data path:{end-start:.2f} seconds")

    print("getting kaggle datasets")
    start = time.time()
    data_ingestion_kaggle.get_kaggle_data(data_path)
    end = time.time()
    print(f"Time elapsed for get_kaggle_data:{end-start:.2f} seconds")

    try:
        print("turning all csv into dataframes")
        start = time.time()
        dataframe_list = data_ingestion_kaggle.retrieve_training_csv(data_path)
        end = time.time()
        print(f"Time elapsed for retrieve_training_csv:{end-start:.2f} seconds")

    except Exception as e:
        print(f"Error when retrieving all csv's as dataframes: {e}")
    
    try:
        print("getting accepted loans csv")
        start = time.time()
        accepted_df = data_ingestion_kaggle.accepted_loans_df(dataframe_list)
        end = time.time()
        print(f"Time elapsed for accepted_loans_df:{end-start:.2f} seconds")
        print(f"length of sample accepted: {len(accepted_df)}")
    except Exception as e:
        print(f"Error when running accepted_loans_df: {e}")

    try:
        print("getting rejected loans csv")
        start = time.time()
        rejected_df = data_ingestion_kaggle.rejected_loans_df(dataframe_list)
        end = time.time()
        print(f"Time elapsed for rejected_loans_df:{end-start:.2f} seconds")
        print(f"length of rejected: {len(rejected_df)}")
    except Exception as e:
        print(f"Error when getting rejected_loans_df: {e}")


    print("deleting large files")
    start = time.time()
    data_ingestion_kaggle.delete_large_files(data_path)
    end = time.time()
    print(f"Time elapsed for delete_large_files:{end-start:.2f} seconds")
    
    print("success")



if __name__ == "__main__":
    print("testing functions")
    testing_all_functions()