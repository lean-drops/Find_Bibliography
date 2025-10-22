# vollständige Vorwärts-Suche (nur Kopf, kein Tail)
python services/find_bibliography.py \
        --head 1.0 \          # = erste 100 % des Dokuments
        --tail 0      \       # = gar keinen Nachlauf untersuchen
"/Users/python/Documents/untitled folder/admin,+zwa_16_01_04.pdf"