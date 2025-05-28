from __future__ import annotations

from pydantic import BaseModel
from typing import List, Optional


class FileMetadataModel(BaseModel):
    """A stable digital asset"""

    class Config:
        arbitrary_types_allowed = True

    # _id: ObjectId

    dcc: DCC

    collections: List[Collection]

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
    # uncompressed_size_in_bytes: Optional[int] = None

    # (string) (preferred) SHA-256 checksum for this file [sha256, md5 cannot both be null]
    sha256: Optional[str] = None

    # (string) (allowed) MD5 checksum for this file [sha256, md5 cannot both be null]
    md5: Optional[str] = None

    # (string) A filename with no prepended PATH information
    filename: str = str()

    # (string) An EDAM CV term ID identifying the digital format of this file (e.g. TSV or FASTQ): if this file is compressed, this should be its _uncompressed_ format
    file_format: Optional[FileFormat] = None

    # (string) An EDAM CV term ID identifying the compression format of this file (e.g. gzip or bzip2): null if this file is not compressed
    compression_format: Optional[str] = None

    # (string) An EDAM CV term ID identifying the type of information stored in this file (e.g. RNA sequence reads): null if is_bundle is set to true
    data_type: Optional[DataType] = None

    # (string) An OBI CV term ID describing the type of experiment that generated the results summarized by this file
    assay_type: Optional[AssayType] = None

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


class DCC(BaseModel):
    """The Common Fund program or data coordinating center (DCC, identified by the given project foreign key) that produced this C2M2 instance"""

    # _id: ObjectId

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


class AssayType(BaseModel):
    """List of Ontology for Biomedical Investigations (OBI) CV terms used to describe types of experiment that generate results stored in C2M2 files"""

    # _id: ObjectId

    # (string) An OBI CV term
    id: str = str()

    # (string) A short, human-readable, machine-read-friendly label for this OBI term
    name: str = str()

    # (string) A human-readable description of this OBI term
    description: Optional[str] = None

    # (array) A list of synonyms for this term as identified by the OBI metadata
    # synonyms: Optional[List[Any]] = None


class FileFormat(BaseModel):
    """List of EDAM CV 'format:' terms used to describe formats of C2M2 files"""

    # _id: ObjectId
    # (string) An EDAM CV format term
    id: str = str()

    # (string) A short, human-readable, machine-read-friendly label for this EDAM format term
    name: str = str()

    # (string) A human-readable description of this EDAM format term
    description: Optional[str] = None

    # (array) A list of synonyms for this term as identified by the EDAM metadata
    # synonyms: Optional[List[Any]] = None


class DataType(BaseModel):
    """List of EDAM CV 'data:' terms used to describe data in C2M2 files"""

    # _id: ObjectId
    # (string) An EDAM CV data term
    id: str = str()

    # (string) A short, human-readable, machine-read-friendly label for this EDAM data term
    name: str = str()

    # (string) A human-readable description of this EDAM data term
    description: Optional[str] = None

    # (array) A list of synonyms for this term as identified by the EDAM metadata
    # synonyms: Optional[List[Any]] = None


class Collection(BaseModel):
    """A grouping of C2M2 files, biosamples and/or subjects"""

    # _id: ObjectId
    biosamples: List[Biosample]

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
    # has_time_series_data: Optional[bool] = None


class Biosample(BaseModel):
    """A tissue sample or other physical specimen"""

    # _id: ObjectId
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
    anatomy: Optional[Anatomy] = None

    # (string) An UBERON CV term or InterLex term used to locate the origin of this biosample within the fluid compartment of its source or host organism
    biofluid: Optional[str] = None


class Anatomy(BaseModel):
    """List of Uber-anatomy ontology (UBERON) CV terms used to locate the origin of a C2M2 biosample within the physiology of its source or host organism"""

    # _id: ObjectId
    # (string) An UBERON CV term
    id: str = str()

    # (string) A short, human-readable, machine-read-friendly label for this UBERON term
    name: str = str()

    # (string) A human-readable description of this UBERON term
    description: Optional[str] = None

    # (array) A list of synonyms for this term as identified by the UBERON metadata
    # synonyms: Optional[List[Any]] = None
