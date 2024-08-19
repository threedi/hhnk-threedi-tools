  
def find_upstream_links_to_breach(result_folder, breach_ids):
    """
    Op basis van een reeds gedownload modelresultaat de breslinks en bijbehorende
    verbindingen bovenstrooms verzamelen.

    Geef de locatie op van de result nc en gridadmin h5.
    Output is een dictionary.
    """
    import os
    import numpy as np
    from threedigrid.admin.gridresultadmin import GridH5ResultAdmin
    # result_folder = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\02.modellen\RegionalFloodModel - deelmodel VRNK WEST\VRNK_WEST_TEST_V6"
    resultnc = os.path.join(result_folder,'results_3di.nc')
    resulth5 = os.path.join(result_folder,'gridadmin.h5')
    gr = GridH5ResultAdmin(resulth5, resultnc)
    #gr.lines._meta.get_fields(only_names=True)

    # list of breaches
    # breach_ids = gr.breaches.content_pk
    d = {'count': len(breach_ids),
        'data':[]}

    for breach_id in breach_ids:
        if breach_id == -9999:
            continue
        else: 
            print(f'finding upstream link to the breach with {breach_id}')
            # find breach node upstream
            breach_mask = gr.breaches.content_pk == breach_id 
            breach_line = gr.breaches.id[breach_mask][0]
            ids = (gr.lines.filter(content_pk=breach_id).id)
            for id in ids:
                type = gr.lines.filter(id__eq=id).content_type[0].decode('UTF-8')
                if type == 'v2_breach':
                    breach_node_upstream = gr.lines.filter(id=id).line[1][0]

            # find breach links upstream
            upstream_end_link_mask = gr.lines.line[1] == breach_node_upstream
            up1 = gr.lines.id[upstream_end_link_mask]
            upstream_link_1 = up1[up1 != breach_line]
            upstream_start_link_mask = gr.lines.line[0] == breach_node_upstream
            up2 = gr.lines.id[upstream_start_link_mask]
            upstream_link_2 = up2[up2 != breach_line]
            upstream_link_ids = np.concatenate((upstream_link_1,upstream_link_2))

            # built dictionary of of upstream links
            types = []
            type = 'onbekend'
            spatialite_ids = []
            for l in upstream_link_ids:
                type = gr.lines.filter(id__eq=l).content_type[0].decode('UTF-8')
                types.append(type)
                spat_id = int(gr.lines.filter(id__eq=l).content_pk)
                spatialite_ids.append(spat_id)
            
            # dict opbouwen
            d_temp = {
                'breach_id': breach_id,
                'breach_line': breach_line,
                'upstream_links': upstream_link_ids,
                'upstream_types': types,
                'spatialite_ids': spatialite_ids
            }
            d['data'].append(d_temp)

    return d

