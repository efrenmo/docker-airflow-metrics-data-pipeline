# modules/update_db_tables.py
from sqlalchemy import create_engine, Table, MetaData, insert
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, OperationalError, IntegrityError
from tenacity import retry, stop_after_attempt, wait_fixed
import logging
from modules.utilities import setup_logging
from modules.database_operations import get_db_conn_w_sqlalchemy


logger = setup_logging(__name__)

# # Set up logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(3), wait=wait_fixed(1))
def execute_with_retry(session, stmt):
    try:
        result = session.execute(stmt)
        session.commit()
        return result
    except OperationalError as e:
        session.rollback()
        logger.error(f"OperationalError occurred: {e}")
        raise e

def validate_types(data, table):
    validated_data = {}
    for column, value in data.items():
        if column in table.columns:  # Check if the column exists in the table
            col_type = table.columns[column].type
            if value is None:
                validated_data[column] = None
            elif isinstance(col_type, String):
                validated_data[column] = str(value)
            elif isinstance(col_type, Integer):
                if not isinstance(value, int):
                    try:
                        validated_data[column] = int(value)
                    except ValueError:
                        print(f"Warning: Could not convert {value} to integer for column {column}")
                        validated_data[column] = None
                else:
                    validated_data[column] = value
            elif isinstance(col_type, Float):
                if not isinstance(value, float):
                    try:
                        validated_data[column] = float(value)
                    except ValueError:
                        print(f"Warning: Could not convert {value} to float for column {column}")
                        validated_data[column] = None
                else:
                    validated_data[column] = value
            elif isinstance(col_type, Date):
                if not isinstance(value, (datetime, pd.Timestamp)):
                    try:
                        validated_data[column] = pd.to_datetime(value).normalize().date()
                    except ValueError:
                        print(f"Warning: Could not convert {value} to date for column {column}")
                        validated_data[column] = None
                else:
                    validated_data[column] = value.date() if isinstance(value, pd.Timestamp) else value
            else:
                validated_data[column] = value  # For any other types, keep as is
    return validated_data


def process_batch(batch, table, Session):
    validated_batch = []
    for record in batch:
        try:
            validated_record = validate_types(record, table)
            if validated_record:
                validated_batch.append(validated_record)
        except Exception as e:
            logger.error(f"Error validating record: {e}")
    
    if validated_batch:
        with Session() as session:
            stmt = insert(table).values(validated_batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=['hash_id'],
                set_={
                    'reference_number': stmt.excluded.reference_number,
                    'group_reference': stmt.excluded.group_reference,
                    'parent_model': stmt.excluded.parent_model,
                    'specific_model': stmt.excluded.specific_model,
                    'year_introduced': stmt.excluded.year_introduced
                }
            )
            try:
                result = execute_with_retry(session, stmt)
                logger.info(f"Batch processed successfully. Rows affected: {result.rowcount}")
            except IntegrityError as e:
                """
                This approach will log all hash_ids in the batch where the IntegrityError occurred. 
                However, it's important to note that this doesn't necessarily mean all these hash_ids failed - 
                it just means at least one in the batch did.
                """
                failed_hash_ids = [record['hash_id'] for record in validated_batch]            
                logger.error(f"IntegrityError occurred. Failed hash_ids: {failed_hash_ids}")
                logger.error(f"Error details: {str(e)}")
            except SQLAlchemyError as e:
                logger.error(f"An error occurred: {e}")
    else:
        logger.warning(f"No valid records in batch")

def update_db_table_wcc_data_enriched(wcc_df_enriched, batch_size=50000):
    engine = get_db_conn_w_sqlalchemy()
    Session = sessionmaker(bind=engine)
    table = Table('wcc_data_enriched', MetaData(), autoload_with=engine)
    
    updated_data = wcc_df_enriched.to_dict(orient='records')
    
    for i in range(0, len(updated_data), batch_size):
        batch = updated_data[i:i+batch_size]
        logger.info(f"Processing batch starting at index {i}")
        process_batch(batch, table, Session)
    
    logger.info("All batches processed")

# Main execution
if __name__ == "__main__":    
    update_wcc_data_enriched(wcc_df_enriched)