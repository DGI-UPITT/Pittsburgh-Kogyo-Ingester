# -*- coding: utf8 -*-
from islandoraUtils import fileConverter as converter
from islandoraUtils import fileManipulator as manipulator
from utils.commonFedora import *
import subprocess, glob, sys, zipfile

#Get etree from somewhere it should be...
try:
    from lxml import etree
    print("running with lxml.etree")
except ImportError:
    try:
        # Python 2.5
        import xml.etree.cElementTree as etree
        print("running with cElementTree on Python 2.5+")
    except ImportError:
        try:
            # Python 2.5
            import xml.etree.ElementTree as etree
            print("running with ElementTree on Python 2.5+")
        except ImportError:
            try:
                # normal cElementTree install
                import cElementTree as etree
                print("running with cElementTree")
            except ImportError:
                try:
                    # normal ElementTree install
                    import elementtree.ElementTree as etree
                    print("running with ElementTree")
                except ImportError:
                    message = "Failed to import ElementTree from any known place"
                    print(message)
                    raise ImportError(message)

""" ====== INGEST A SINGLE OBJECT ====== """
def createObjectFromFiles(fedora, config, objectData):
    """
    Create a fedora object containing all the data in objectData and more
    """

    bookFolder = config.inDir

    for ds in [ "DC", "MODS", "MARC", "METS" ]:
        # some error checking
        if not objectData['datastreams'][ds]:
            # broken object
            print("Object data is missing required datastream: %s" % ds)
            return False

    objPid = "%s:%s" % (config.fedoraNS, objectData['label'])

    if not config.dryrun:
        # create the object (page)
        try:
            bookObj = addCollectionToFedora(fedora, unicode("%s" % objectData['label']), objPid, objectData['parentPid'], objectData['contentModel'])
        except FedoraConnectionException, fcx:
            print("Connection error while trying to add fedora object (%s) - the connection to fedora may be broken", objPid)
            return False

        # ingest the datastreams we were given
        for dsid, file in objectData['datastreams'].iteritems():
            # hard coded blarg:
            if dsid in ["MODS"]: # maybe make all these 'X'
                controlGroup = "X"
            else:
                controlGroup = "M"
            fedoraLib.update_datastream(bookObj, dsid, file, label=unicode(os.path.basename(file)), mimeType=misc.getMimeType(os.path.splitext(file)[1]), controlGroup=controlGroup)

    # ingest my custom datastreams for this object

    # scan for pages using the mets record
    print("Page index from file %s" % objectData['datastreams']['METS'])
    parser = etree.XMLParser(remove_blank_text=True)
    pageIndex = etree.parse(objectData['datastreams']['METS'], parser)

    nsmap = { 'mets' : 'http://www.loc.gov/METS/', 'xlink' : 'http://www.w3.org/1999/xlink' }

    pages = pageIndex.xpath("//mets:structMap/mets:div/mets:div/*", namespaces=nsmap)
    fullPageData = []
    for p in pages:
        label = p.attrib['LABEL']
        fileid = p.xpath("mets:fptr", namespaces=nsmap)[0].attrib['FILEID']
        filename = pageIndex.xpath("//mets:fileSec/mets:fileGrp/mets:file[@ID='%s']/mets:FLocat" % fileid, namespaces=nsmap)[0].attrib['{%s}href' % nsmap['xlink']]
        tup = (fileid, label, os.path.join(os.path.splitext(filename)[0], filename))
        fullPageData.append(tup)
        print("fileid=%s, label='%s' filename='%s'" % tup)
    fullPageData.sort(key=lambda tup: tup[0])
    count = len(fullPageData)

    # book thumbnail is the first image
    if not config.dryrun:
        tnFile = os.path.join(config.tempDir, "tmp.jpg")
        converter.tif_to_jpg(os.path.join(bookFolder, fullPageData[0][2]), tnFile, imageMagicOpts='TN')
        #add a TN datastream to the book
        fedoraLib.update_datastream(bookObj, u"TN", tnFile, label=unicode(config.myCollectionName+"_TN.jpg"), mimeType=misc.getMimeType("jpg"))

    print("Build kogyo book object with %d pages" % count)

    baseName = objectData['label']

    fullPDF = os.path.join(config.tempDir, "%s.pdf" % baseName)

    for idx, pageset in enumerate(fullPageData):
        page = pageset[2]
        print("\n==========\nIngesting object %d of %d: %s" % (idx+1, count, page))

        basePage = os.path.splitext(os.path.basename(page))[0]

        pagePid = "%s-%d" % (objPid, idx+1) # objPid contains the namespace part of the pid

        extraNamespaces = { 'pageNS' : 'info:islandora/islandora-system:def/pageinfo#' }
        extraRelationships = { fedora_relationships.rels_predicate('pageNS', 'isPageNumber') : str(idx+1) }

        if not config.dryrun:
            # create the object (page)
            try:
                obj = addObjectToFedora(fedora, u"""""unicode(pageset[1])""", pagePid, objPid, "archiveorg:pageCModel",
                        extraNamespaces=extraNamespaces, extraRelationships=extraRelationships)
            except FedoraConnectionException, fcx:
                print("Connection error while trying to add fedora object (%s) - the connection to fedora may be broken", page)
                continue

            # ingest the tif
            tifFile = os.path.join(bookFolder, page)
            fedoraLib.update_datastream(obj, u"TIFF", tifFile, label=unicode("%s.tif" % basePage), mimeType=misc.getMimeType("tiff"))

            # create a JP2 datastream
            jp2File = os.path.join(config.tempDir, "%s.jp2" % basePage)
            converter.tif_to_jp2(tifFile, jp2File, 'default', 'default')
            fedoraLib.update_datastream(obj, u"JP2", jp2File, label=unicode("%s.jp2" % basePage), mimeType=misc.getMimeType("jp2"))
            os.remove(jp2File) # finished with that

            # create DC, MODS, VRA datastreams
            for dsid in ['DC', 'MODS', 'VRA']:
                dsfile = os.path.join(bookFolder, "%s.%s.xml" % (os.path.splitext(page)[0], dsid.lower()))
                dspage = os.path.basename(dsfile)
                fedoraLib.update_datastream(obj, unicode(dsid), dsfile, label=unicode(dspage), mimeType=misc.getMimeType("xml"), controlGroup='X')
            """
            pdfFile = os.path.join(config.tempDir, "%s.pdf" % basePage)
            converter.tif_to_pdf(tifFile, pdfFile, ['-q', '25'])
            fedoraLib.update_datastream(obj, u'PDF', pdfFile, label=unicode("%s.pdf" % basePage), mimeType=misc.getMimeType("pdf"))
            # for the first page, move it to the full when finished with it
            if idx == 0:
                os.rename(pdfFile, fullPDF)
            # for every other page (>1), append it to fullPDF and delete the original
            else:
                manipulator.appendPDFwithPDF(fullPDF, pdfFile)
                os.remove(pdfFile)
            """
        sys.stdout.flush()
        sys.stderr.flush()
    """
    # ingest the full PDF on the master book object
    # and delete it
    if not config.dryrun:
        print("Ingesting full PDF document")
        fedoraLib.update_datastream(bookObj, u"PDF", fullPDF, label=os.path.basename(fullPDF), mimeType=misc.getMimeType("pdf"))
        os.remove(fullPDF)
    """
    return True
