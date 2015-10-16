select trim(leading '0' from old_key) oldkey, trim(leading '0' from new_key) new_key
from tidldkeysql
where table_id = 'TIDARMST'
