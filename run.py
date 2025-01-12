from app import UI_DATA_OBJ
import logger as log
from fda_drugs import (
    DrugsMeta
)
from dailymed import (
    DailyMed
)

def fetch_fda_drugs():
    try:
        drugs_meta_obj = DrugsMeta()
        drugs_meta_obj.update_drugs_data_to_db(download_new_data=True)
    except Exception as exc:
        log.do_error(f"Error while fetching drugs from FDA drugs portal, stopping execution!")
        raise exc

def mark_lyophilized_drugs_in_db():
    try:
        dailymed_obj = DailyMed()
        dailymed_obj.update_drugs_setids_to_db()
        dailymed_obj.insert_drugs_to_lyophilized_coll()
        dailymed_obj.compare_dailymed_data_from_db()
    except Exception as exc:
        log.do_error(f"Exception while updating lyophilized tags for drugs in mongo collections, stopping execution!")
        raise exc

def get_ingredients_for_lyophilized():
    try:
        dailymed_obj = DailyMed()
        dailymed_obj.update_ingredients_to_db()
    except Exception as exc:
        log.do_error(f"Failed to fetch ingredients for lyophilized drugs, stopping execution!")
        raise exc

def run_backend():
    try:
        fetch_fda_drugs()
        mark_lyophilized_drugs_in_db()
        get_ingredients_for_lyophilized()
        log.do_info("Data pipeline completed")
    except Exception as exc:
        log.do_error(f"Pipeline error: {str(exc)}")
        raise exc

if __name__ == "__main__":
    run_backend()
    print("test")
    