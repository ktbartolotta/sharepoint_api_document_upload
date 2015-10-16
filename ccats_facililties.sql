select regionds facility
from impact.dbo.ppl_vw_talen_incident
union
select oshasiteds facility
from impact.dbo.ppl_vw_talen_assessment
union
select oshasite facility
from impact.dbo.ppl_vw_talen_finding
union
select regionds facility
from impact.dbo.ppl_vw_talen_vehicle
union
select oshasite facility
from impact.dbo.ppl_vw_talen_generation
union
select oshasite facility
from impact.dbo.ppl_vw_talen_environmental
union
select oshasite facility
from impact.dbo.ppl_vw_talen_medical
union
select oshasite facility
from impact.dbo.ppl_vw_talen_property
union
select osha_site facility
from impact.dbo.ppl_vw_talen_investigation
union
select oshasiteds facility
from impact.dbo.ppl_vw_talen_actionitem
