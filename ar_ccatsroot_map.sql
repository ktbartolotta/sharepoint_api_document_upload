select trim(reference_table) reference_key,
    trim(leading '0' from old_key) oldkey,
    trim(leading '0' from new_key) new_key
from tidldkey
where table_id = 'TIDARMST'
