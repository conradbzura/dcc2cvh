# This file was generated automatically from a frictionless datapackage
#  using frictionless-dataclass.

from dataclasses import dataclass
from typing import Any, List, Optional


class table_schema_specs_for_c2m2_encoding_of_dcc_metadata:
    """A complete list of schematic specifications for the resources (TSV table files) that will be used to represent C2M2 DCC metadata prior to ingest into the C2M2 database system"""

    @dataclass
    class file:
        """A stable digital asset"""

        # (string) A CFDE-cleared identifier representing the top-level data space containing this file [part 1 of 2-component composite primary key]
        id_namespace: str = str()

        # (string) An identifier representing this file, unique within this id_namespace [part 2 of 2-component composite primary key]
        local_id: str = str()

        # (string) The id_namespace of the primary project within which this file was created [part 1 of 2-component composite foreign key]
        project_id_namespace: str = str()

        # (string) The local_id of the primary project within which this file was created [part 2 of 2-component composite foreign key]
        project_local_id: str = str()

        # (string) A persistent, resolvable (not necessarily retrievable) URI or compact ID permanently attached to this file
        persistent_id: Optional[str] = None

        # (datetime) An ISO 8601 -- RFC 3339 (subset)-compliant timestamp documenting this file's creation time: YYYY-MM-DDTHH:MM:SS±NN:NN
        creation_time: Optional[str] = None

        # (integer) The size of this file in bytes
        size_in_bytes: Optional[int] = None

        # (integer) The total decompressed size in bytes of the contents of this file: null if this file is not compressed
        uncompressed_size_in_bytes: Optional[int] = None

        # (string) (preferred) SHA-256 checksum for this file [sha256, md5 cannot both be null]
        sha256: Optional[str] = None

        # (string) (allowed) MD5 checksum for this file [sha256, md5 cannot both be null]
        md5: Optional[str] = None

        # (string) A filename with no prepended PATH information
        filename: str = str()

        # (string) An EDAM CV term ID identifying the digital format of this file (e.g. TSV or FASTQ): if this file is compressed, this should be its _uncompressed_ format
        file_format: Optional[str] = None

        # (string) An EDAM CV term ID identifying the compression format of this file (e.g. gzip or bzip2): null if this file is not compressed
        compression_format: Optional[str] = None

        # (string) An EDAM CV term ID identifying the type of information stored in this file (e.g. RNA sequence reads): null if is_bundle is set to true
        data_type: Optional[str] = None

        # (string) An OBI CV term ID describing the type of experiment that generated the results summarized by this file
        assay_type: Optional[str] = None

        # (string) An OBI CV term ID describing the type of analytic operation that generated this file
        analysis_type: Optional[str] = None

        # (string) A MIME type describing this file
        mime_type: Optional[str] = None

        # (string) If this file is a bundle encoding more than one sub-file, this field gives the id_namespace of a collection listing the bundle's sub-file contents; null otherwise
        bundle_collection_id_namespace: Optional[str] = None

        # (string) If this file is a bundle encoding more than one sub-file, this field gives the local_id of a collection listing the bundle's sub-file contents; null otherwise
        bundle_collection_local_id: Optional[str] = None

        # (string) The name of a dbGaP study ID governing access control for this file, compatible for comparison to RAS user-level access control metadata
        dbgap_study_id: Optional[str] = None

        # (string) A DRS URI or otherwise a publicly accessible DRS-compatible URL
        access_url: Optional[str] = None

    @dataclass
    class biosample:
        """A tissue sample or other physical specimen"""

        # (string) A CFDE-cleared identifier representing the top-level data space containing this biosample [part 1 of 2-component composite primary key]
        id_namespace: str = str()

        # (string) An identifier representing this biosample, unique within this id_namespace [part 2 of 2-component composite primary key]
        local_id: str = str()

        # (string) The id_namespace of the primary project within which this biosample was created [part 1 of 2-component composite foreign key]
        project_id_namespace: str = str()

        # (string) The local_id of the primary project within which this biosample was created [part 2 of 2-component composite foreign key]
        project_local_id: str = str()

        # (string) A persistent, resolvable (not necessarily retrievable) URI or compact ID permanently attached to this biosample
        persistent_id: Optional[str] = None

        # (datetime) An ISO 8601 -- RFC 3339 (subset)-compliant timestamp documenting this biosample's creation time: YYYY-MM-DDTHH:MM:SS±NN:NN
        creation_time: Optional[str] = None

        # (string) An OBI CV term ID (from the 'planned process' branch of the vocabulary, excluding the 'assay' subtree) describing the preparation method that produced this biosample
        sample_prep_method: Optional[str] = None

        # (string) An UBERON CV term ID used to locate the origin of this biosample within the physiology of its source or host organism
        anatomy: Optional[str] = None

        # (string) An UBERON CV term or InterLex term used to locate the origin of this biosample within the fluid compartment of its source or host organism
        biofluid: Optional[str] = None

    @dataclass
    class subject:
        """A biological entity from which a C2M2 biosample can in principle be generated"""

        # (string) A CFDE-cleared identifier representing the top-level data space containing this subject [part 1 of 2-component composite primary key]
        id_namespace: str = str()

        # (string) An identifier representing this subject, unique within this id_namespace [part 2 of 2-component composite primary key]
        local_id: str = str()

        # (string) The id_namespace of the primary project within which this subject was studied [part 1 of 2-component composite foreign key]
        project_id_namespace: str = str()

        # (string) The local_id of the primary project within which this subject was studied [part 2 of 2-component composite foreign key]
        project_local_id: str = str()

        # (string) A persistent, resolvable (not necessarily retrievable) URI or compact ID permanently attached to this subject
        persistent_id: Optional[str] = None

        # (datetime) An ISO 8601 -- RFC 3339 (subset)-compliant timestamp documenting this subject record's creation time: YYYY-MM-DDTHH:MM:SS±NN:NN
        creation_time: Optional[str] = None

        # (string) A CFDE CV category characterizing this subject by multiplicity
        granularity: str = str()

        # (string) A CFDE CV category characterizing the physiological sex of this subject
        sex: Optional[str] = None

        # (string) A CFDE CV category characterizing the self-reported ethnicity of this subject
        ethnicity: Optional[str] = None

        # (number) The age in years (with a fixed precision of two digits past the decimal point) of this subject when they were first enrolled in the primary project within which they were studied
        age_at_enrollment: Optional[float] = None

    @dataclass
    class dcc:
        """The Common Fund program or data coordinating center (DCC, identified by the given project foreign key) that produced this C2M2 instance"""

        # (string) The identifier for this DCC, issued by the CFDE-CC
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this DCC
        dcc_name: str = str()

        # (string) A very short display label for this contact's DCC
        dcc_abbreviation: str = str()

        # (string) A human-readable description of this DCC
        dcc_description: Optional[str] = None

        # (string) Email address of this DCC's primary technical contact
        contact_email: str = str()

        # (string) Name of this DCC's primary technical contact
        contact_name: str = str()

        # (string) URL of the front page of the website for this DCC
        dcc_url: str = str()

        # (string) ID of the identifier namespace for the project record representing the C2M2 submission produced by this DCC
        project_id_namespace: str = str()

        # (string) Foreign key identifying the project record representing the C2M2 submission produced by this DCC
        project_local_id: str = str()

    @dataclass
    class project:
        """A node in the C2M2 project hierarchy subdividing all resources described by this DCC's C2M2 metadata"""

        # (string) A CFDE-cleared identifier representing the top-level data space containing this project [part 1 of 2-component composite primary key]
        id_namespace: str = str()

        # (string) An identifier representing this project, unique within this id_namespace [part 2 of 2-component composite primary key]
        local_id: str = str()

        # (string) A persistent, resolvable (not necessarily retrievable) URI or compact ID permanently attached to this project
        persistent_id: Optional[str] = None

        # (datetime) An ISO 8601 -- RFC 3339 (subset)-compliant timestamp documenting this project's creation time: YYYY-MM-DDTHH:MM:SS±NN:NN
        creation_time: Optional[str] = None

        # (string) A very short display label for this project
        abbreviation: Optional[str] = None

        # (string) A short, human-readable, machine-read-friendly label for this project
        name: str = str()

        # (string) A human-readable description of this project
        description: Optional[str] = None

    @dataclass
    class project_in_project:
        """Association between a child project and its parent"""

        # (string) ID of the identifier namespace for the parent in this parent-child project pair
        parent_project_id_namespace: str = str()

        # (string) The ID of the containing (parent) project
        parent_project_local_id: str = str()

        # (string) ID of the identifier namespace for the child in this parent-child project pair
        child_project_id_namespace: str = str()

        # (string) The ID of the contained (child) project
        child_project_local_id: str = str()

    @dataclass
    class collection:
        """A grouping of C2M2 files, biosamples and/or subjects"""

        # (string) A CFDE-cleared identifier representing the top-level data space containing this collection [part 1 of 2-component composite primary key]
        id_namespace: str = str()

        # (string) An identifier representing this collection, unique within this id_namespace [part 2 of 2-component composite primary key]
        local_id: str = str()

        # (string) A persistent, resolvable (not necessarily retrievable) URI or compact ID permanently attached to this collection
        persistent_id: Optional[str] = None

        # (datetime) An ISO 8601 -- RFC 3339 (subset)-compliant timestamp documenting this collection's creation time: YYYY-MM-DDTHH:MM:SS±NN:NN
        creation_time: Optional[str] = None

        # (string) A very short display label for this collection
        abbreviation: Optional[str] = None

        # (string) A short, human-readable, machine-read-friendly label for this collection
        name: str = str()

        # (string) A human-readable description of this collection
        description: Optional[str] = None

        # (boolean) Does this collection contain time-series data? (allowed values: [true|false|null] -- true == yes, contains time-series data; false == no, doesn't contain time-series data; null == no info provided)
        has_time_series_data: Optional[bool] = None

    @dataclass
    class collection_in_collection:
        """Association between a containing collection (superset) and a contained collection (subset)"""

        # (string) ID of the identifier namespace corresponding to the C2M2 submission containing the superset collection
        superset_collection_id_namespace: str = str()

        # (string) The ID of the superset collection
        superset_collection_local_id: str = str()

        # (string) ID of the identifier namespace corresponding to the C2M2 submission containing the subset collection
        subset_collection_id_namespace: str = str()

        # (string) The ID of the subset collection
        subset_collection_local_id: str = str()

    @dataclass
    class file_describes_collection:
        """Association between a summary file and an entire collection described by that file"""

        # (string) Identifier namespace for this file
        file_id_namespace: str = str()

        # (string) The ID of this file
        file_local_id: str = str()

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

    @dataclass
    class collection_defined_by_project:
        """(Shallow) association between a collection and a project that defined it"""

        # (string) ID of the identifier namespace corresponding to the C2M2 submission containing this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

        # (string) ID of the identifier namespace corresponding to the C2M2 submission containing this project
        project_id_namespace: str = str()

        # (string) The ID of this project
        project_local_id: str = str()

    @dataclass
    class file_in_collection:
        """Association between a file and a (containing) collection"""

        # (string) Identifier namespace for this file
        file_id_namespace: str = str()

        # (string) The ID of this file
        file_local_id: str = str()

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

    @dataclass
    class biosample_in_collection:
        """Association between a biosample and a (containing) collection"""

        # (string) Identifier namespace for this biosample
        biosample_id_namespace: str = str()

        # (string) The ID of this biosample
        biosample_local_id: str = str()

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

    @dataclass
    class subject_in_collection:
        """Association between a subject and a (containing) collection"""

        # (string) Identifier namespace for this subject
        subject_id_namespace: str = str()

        # (string) The ID of this subject
        subject_local_id: str = str()

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

    @dataclass
    class file_describes_biosample:
        """Association between a biosample and a file containing information about that biosample"""

        # (string) Identifier namespace for this file
        file_id_namespace: str = str()

        # (string) The ID of this file
        file_local_id: str = str()

        # (string) Identifier namespace for this biosample
        biosample_id_namespace: str = str()

        # (string) The ID of this biosample
        biosample_local_id: str = str()

    @dataclass
    class file_describes_subject:
        """Association between a subject and a file containing information about that subject"""

        # (string) Identifier namespace for this file
        file_id_namespace: str = str()

        # (string) The ID of this file
        file_local_id: str = str()

        # (string) Identifier namespace for this subject
        subject_id_namespace: str = str()

        # (string) The ID of this subject
        subject_local_id: str = str()

    @dataclass
    class biosample_from_subject:
        """Association between a biosample and its source subject"""

        # (string) Identifier namespace for this biosample
        biosample_id_namespace: str = str()

        # (string) The ID of this biosample
        biosample_local_id: str = str()

        # (string) Identifier namespace for this subject
        subject_id_namespace: str = str()

        # (string) The ID of this subject
        subject_local_id: str = str()

        # (number) The age in years (with a fixed precision of two digits past the decimal point) of this subject when this biosample was taken
        age_at_sampling: Optional[float] = None

    @dataclass
    class biosample_disease:
        """Association between a C2M2 biosample and a disease positively (e.g. cancer tumor tissue sample) OR negatively (e.g. cancer-free tissue sample) identified for that biosample"""

        # (string) Identifier namespace for this biosample
        biosample_id_namespace: str = str()

        # (string) The ID of this biosample
        biosample_local_id: str = str()

        # (string) The relationship between this biosample and this disease (e.g. 'observed' or '(tested for, but) not observed')
        association_type: str = str()

        # (string) A Disease Ontology CV term ID describing this disease
        disease: str = str()

    @dataclass
    class subject_disease:
        """Association between a C2M2 subject and a disease positively OR negatively clinically identified in that subject"""

        # (string) Identifier namespace for this subject
        subject_id_namespace: str = str()

        # (string) The ID of this subject
        subject_local_id: str = str()

        # (string) The relationship between this subject and this disease (e.g. 'observed' or '(tested for, but) not observed')
        association_type: str = str()

        # (string) A Disease Ontology CV term ID describing this disease
        disease: str = str()

    @dataclass
    class collection_disease:
        """Association between a disease and a C2M2 collection containing experimental resources directly related to the study of that disease"""

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

        # (string) A Disease Ontology CV term ID describing this disease
        disease: str = str()

    @dataclass
    class collection_phenotype:
        """Association between a phenotype and a C2M2 collection containing experimental resources directly related to the study of that phenotype"""

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

        # (string) A Human Phenotype Ontology CV term ID describing this phenotype
        phenotype: str = str()

    @dataclass
    class collection_gene:
        """Association between a gene and a C2M2 collection containing experimental resources directly related to the study of that gene"""

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

        # (string) An Ensembl term ID describing this gene
        gene: str = str()

    @dataclass
    class collection_compound:
        """Association between a compound and a C2M2 collection containing experimental resources directly related to the study of that compound"""

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

        # (string) A PubChem or GlyTouCan term ID describing this compound
        compound: str = str()

    @dataclass
    class collection_substance:
        """Association between a substance and a C2M2 collection containing experimental resources directly related to the study of that substance"""

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

        # (string) A PubChem term ID describing this substance
        substance: str = str()

    @dataclass
    class collection_taxonomy:
        """Association between a taxon and a C2M2 collection containing experimental resources directly related to the study of that taxon"""

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

        # (string) An NCBI Taxonomy Database ID identifying this taxon
        taxon: str = str()

    @dataclass
    class collection_anatomy:
        """Association between an UBERON anatomical term and a C2M2 collection containing experimental resources directly related to the study of the anatomical concept described by that term"""

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

        # (string) An UBERON term ID
        anatomy: str = str()

    @dataclass
    class collection_biofluid:
        """Association between an UBERON/InterLex biofluid term and a C2M2 collection containing experimental resources directly related to the study of the biofluid concept described by that term"""

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

        # (string) An UBERON term ID
        biofluid: str = str()

    @dataclass
    class collection_protein:
        """Association between a protein and a C2M2 collection containing experimental resources directly related to the study of that protein"""

        # (string) Identifier namespace for this collection
        collection_id_namespace: str = str()

        # (string) The ID of this collection
        collection_local_id: str = str()

        # (string) A UniProtKB term ID describing this protein
        protein: str = str()

    @dataclass
    class subject_phenotype:
        """Association between a C2M2 subject and a phenotype positively OR negatively clinically identified for that subject"""

        # (string) Identifier namespace for this subject
        subject_id_namespace: str = str()

        # (string) The ID of this subject
        subject_local_id: str = str()

        # (string) The relationship between this subject and this phenotype (e.g. 'observed' or '(tested for, but) not observed')
        association_type: str = str()

        # (string) A Human Phenotype Ontology CV term ID describing this phenotype
        phenotype: str = str()

    @dataclass
    class biosample_substance:
        """Association between a C2M2 biosample and a PubChem substance experimentally associated with that biosample"""

        # (string) Identifier namespace for this biosample
        biosample_id_namespace: str = str()

        # (string) The ID of this biosample
        biosample_local_id: str = str()

        # (string) A PubChem substance ID (SID) describing this substance
        substance: str = str()

    @dataclass
    class subject_substance:
        """Association between a C2M2 subject and a PubChem substance experimentally associated with that subject"""

        # (string) Identifier namespace for this subject
        subject_id_namespace: str = str()

        # (string) The ID of this subject
        subject_local_id: str = str()

        # (string) A PubChem substance ID (SID) describing this substance
        substance: str = str()

    @dataclass
    class biosample_gene:
        """Association between a C2M2 biosample and an Ensembl gene especially relevant to it"""

        # (string) Identifier namespace for this biosample
        biosample_id_namespace: str = str()

        # (string) The ID of this biosample
        biosample_local_id: str = str()

        # (string) An Ensembl gene ID
        gene: str = str()

    @dataclass
    class phenotype_gene:
        """Association between a Human Phenotype Ontology term and an Ensembl gene especially relevant to it"""

        # (string) A Human Phenotype Ontology CV term ID
        phenotype: str = str()

        # (string) An Ensembl gene ID
        gene: str = str()

    @dataclass
    class phenotype_disease:
        """Association between a Human Phenotype Ontology term and a Disease Ontology term identifying a disease especially relevant to it"""

        # (string) A Human Phenotype Ontology CV term ID
        phenotype: str = str()

        # (string) A Disease Ontology CV term ID
        disease: str = str()

    @dataclass
    class subject_race:
        """Identification of a C2M2 subject with one or more self-selected races"""

        # (string) Identifier namespace for this subject
        subject_id_namespace: str = str()

        # (string) The ID of this subject
        subject_local_id: str = str()

        # (string) A race self-identified by this subject
        race: Optional[str] = None

    @dataclass
    class subject_role_taxonomy:
        """Trinary association linking IDs representing (1) a subject, (2) a subject_role (a named organism-level constituent component of a subject, like 'host', 'pathogen', 'endosymbiont', 'taxon detected inside a microbiome subject', etc.) and (3) a taxonomic label (which is hereby assigned to this particular subject_role within this particular subject)"""

        # (string) Identifier namespace for this subject
        subject_id_namespace: str = str()

        # (string) The ID of this subject
        subject_local_id: str = str()

        # (string) The ID of the role assigned to this organism-level constituent component of this subject
        role_id: str = str()

        # (string) An NCBI Taxonomy Database ID identifying this taxon
        taxonomy_id: str = str()

    @dataclass
    class assay_type:
        """List of Ontology for Biomedical Investigations (OBI) CV terms used to describe types of experiment that generate results stored in C2M2 files"""

        # (string) An OBI CV term
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this OBI term
        name: str = str()

        # (string) A human-readable description of this OBI term
        description: Optional[str] = None

        # (array) A list of synonyms for this term as identified by the OBI metadata
        synonyms: Optional[List[Any]] = None

    @dataclass
    class analysis_type:
        """List of Ontology for Biomedical Investigations (OBI) CV terms used to describe analytic methods that generate C2M2 files"""

        # (string) An OBI CV term
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this OBI term
        name: str = str()

        # (string) A human-readable description of this OBI term
        description: Optional[str] = None

        # (array) A list of synonyms for this term as identified by the OBI metadata
        synonyms: Optional[List[Any]] = None

    @dataclass
    class ncbi_taxonomy:
        """List of NCBI Taxonomy Database IDs identifying taxa used to describe C2M2 subjects"""

        # (string) An NCBI Taxonomy Database ID identifying a particular taxon
        id: str = str()

        # (string) The phylogenetic level (e.g. species, genus) assigned to this taxon
        clade: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this taxon
        name: str = str()

        # (string) A human-readable description of this taxon
        description: Optional[str] = None

        # (array) A list of synonyms for this taxon as identified by the NCBI Taxonomy DB
        synonyms: Optional[List[Any]] = None

    @dataclass
    class anatomy:
        """List of Uber-anatomy ontology (UBERON) CV terms used to locate the origin of a C2M2 biosample within the physiology of its source or host organism"""

        # (string) An UBERON CV term
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this UBERON term
        name: str = str()

        # (string) A human-readable description of this UBERON term
        description: Optional[str] = None

        # (array) A list of synonyms for this term as identified by the UBERON metadata
        synonyms: Optional[List[Any]] = None

    @dataclass
    class biofluid:
        """An UBERON CV term or InterLex term used to locate the origin of this biosample within the fluid compartment of its source or host organism"""

        # (string) An UBERON CV term
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this UBERON term
        name: str = str()

        # (string) A human-readable description of this UBERON term
        description: Optional[str] = None

        # (array) A list of synonyms for this term as identified by the UBERON metadata
        synonyms: Optional[List[Any]] = None

    @dataclass
    class file_format:
        """List of EDAM CV 'format:' terms used to describe formats of C2M2 files"""

        # (string) An EDAM CV format term
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this EDAM format term
        name: str = str()

        # (string) A human-readable description of this EDAM format term
        description: Optional[str] = None

        # (array) A list of synonyms for this term as identified by the EDAM metadata
        synonyms: Optional[List[Any]] = None

    @dataclass
    class data_type:
        """List of EDAM CV 'data:' terms used to describe data in C2M2 files"""

        # (string) An EDAM CV data term
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this EDAM data term
        name: str = str()

        # (string) A human-readable description of this EDAM data term
        description: Optional[str] = None

        # (array) A list of synonyms for this term as identified by the EDAM metadata
        synonyms: Optional[List[Any]] = None

    @dataclass
    class disease:
        """List of Disease Ontology terms used to describe diseases recorded in association with C2M2 subjects or biosamples"""

        # (string) A Disease Ontology term
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this Disease Ontology term
        name: str = str()

        # (string) A human-readable description of this Disease Ontology term
        description: Optional[str] = None

        # (array) A list of synonyms for this term as identified by the Disease Ontology metadata
        synonyms: Optional[List[Any]] = None

    @dataclass
    class phenotype:
        """List of Human Phenotype Ontology terms used to describe phenotypes recorded in association with C2M2 subjects"""

        # (string) A Human Phenotype Ontology term
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this Human Phenotype Ontology term
        name: str = str()

        # (string) A human-readable description of this Human Phenotype Ontology term
        description: Optional[str] = None

        # (array) A list of synonyms for this term as identified by the Human Phenotype Ontology metadata
        synonyms: Optional[List[Any]] = None

    @dataclass
    class compound:
        """List of (i) GlyTouCan terms or (ii) PubChem 'compound' terms (normalized chemical structures) referenced in this submission; (ii) will include all PubChem 'compound' terms associated with any PubChem 'substance' terms (specific formulations of chemical materials) directly referenced in this submission, in addition to any 'compound' terms directly referenced"""

        # (string) A GlyTouCan ID or a PubChem compound ID (CID)
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this compound
        name: str = str()

        # (string) A human-readable description of this compound
        description: Optional[str] = None

        # (array) A list of synonyms for this compound
        synonyms: Optional[List[Any]] = None

    @dataclass
    class substance:
        """List of PubChem 'substance' terms (specific formulations of chemical materials) directly referenced in this C2M2 submission"""

        # (string) A PubChem substance ID (SID)
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this PubChem SID
        name: str = str()

        # (string) A human-readable description of this PubChem SID
        description: Optional[str] = None

        # (array) A list of synonyms for this PubChem SID
        synonyms: Optional[List[Any]] = None

        # (string) The (unique) PubChem compound ID (CID) associated with this PubChem SID
        compound: str = str()

    @dataclass
    class gene:
        """List of Ensembl genes directly referenced in this C2M2 submission"""

        # (string) An Ensembl gene ID (e.g. 'ENSG00000012048')
        id: str = str()

        # (string) The Ensembl 'Name' for this gene (e.g. 'BRCA1')
        name: str = str()

        # (string) The Ensembl 'Description' of this gene (e.g. 'BRCA1 DNA repair associated')
        description: Optional[str] = None

        # (array) A list of Ensembl 'Gene synonyms' for this gene (e.g. ['BRCC1', 'FANCS', 'PPP1R53', 'RNF53'])
        synonyms: Optional[List[Any]] = None

        # (string) An NCBI Taxonomy Database ID identifying this gene's source organism (e.g. 'NCBI:txid9606')
        organism: str = str()

    @dataclass
    class protein:
        """List of UniProtKB proteins directly referenced in this C2M2 submission"""

        # (string) A UniProt Knowledgebase (UniProtKB) protein ID (e.g. 'P94485')
        id: str = str()

        # (string) The UniProt recommended name of this protein (e.g. 'Uncharacterized protein YnaG')
        name: str = str()

        # (string) A description of this protein
        description: Optional[str] = None

        # (array) A list of alternate names for this protein
        synonyms: Optional[List[Any]] = None

        # (string) OPTIONAL: An NCBI Taxonomy Database ID identifying this protein's source organism (e.g. 'NCBI:txid9606')
        organism: Optional[str] = None

    @dataclass
    class protein_gene:
        """Association between a UniProtKB protein term and an Ensembl term identifying a gene encoding that protein"""

        # (string) A UniProt Knowledgebase (UniProtKB) protein ID (e.g. 'P94485')
        protein: str = str()

        # (string) An Ensembl gene ID (e.g. 'ENSG00000012048')
        gene: str = str()

    @dataclass
    class sample_prep_method:
        """List of Ontology for Biomedical Investigations (OBI) CV terms used to describe types of preparation methods that produce C2M2 biosamples"""

        # (string) An OBI CV term
        id: str = str()

        # (string) A short, human-readable, machine-read-friendly label for this OBI term
        name: str = str()

        # (string) A human-readable description of this OBI term
        description: Optional[str] = None

        # (array) A list of synonyms for this term as identified by the OBI metadata
        synonyms: Optional[List[Any]] = None

    @dataclass
    class id_namespace:
        """A table listing identifier namespaces registered by the DCC submitting this C2M2 instance"""

        # (string) ID of this identifier namespace
        id: str = str()

        # (string) A very short display label for this identifier namespace
        abbreviation: Optional[str] = None

        # (string) A short, human-readable, machine-read-friendly label for this identifier namespace
        name: str = str()

        # (string) A human-readable description of this identifier namespace
        description: Optional[str] = None
