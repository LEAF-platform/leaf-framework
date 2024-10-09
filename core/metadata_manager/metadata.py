import json
import yaml
import os
import re

equipment_key = "equipment"

class MetadataManager:

    def __init__(self):
        """Initialize the metadata dictionary for each adapter."""
        self._metadata = {}
        self.equipment_terms = None
        self.load_equipment_terms()

    def load_equipment_terms(self):
        """Load YAML configuration into equipment terms."""
        curr_dir = os.path.dirname(os.path.realpath(__file__))
        filepath = os.path.join(curr_dir, "equipment_actions.yaml")
        try:
            with open(filepath, "r") as file:
                yaml_content = yaml.safe_load(file)
                self.equipment_terms = EquipmentTerms(yaml_content, 
                                                      self._metadata)
        except FileNotFoundError:
            print(f"YAML file {filepath} not found.")

    def load_from_file(self, file_path, adapter_type=None):
        """Load metadata from a JSON file and update the metadata dictionary."""
        try:
            with open(file_path, "r") as file:
                if adapter_type is not None:
                    self._metadata[adapter_type].update(json.load(file))
                else:
                    self._metadata.update(json.load(file))
        except FileNotFoundError:
            print(f"Metadata file {file_path} not found.")

    def get_metadata(self, key, default=None):
        """Retrieve a specific metadata value."""
        return self._metadata.get(key, default)

    def set_metadata(self, key, value):
        """Set a specific metadata value."""
        self._metadata[key] = value

    def get_equipment_data(self):
        if equipment_key in self._metadata:
            return self._metadata[equipment_key]

    def add_equipment_data(self, filename):
        if isinstance(filename, dict):
            return self._metadata[equipment_key].update(filename)
        else:
            return self.load_from_file(filename, equipment_key)

    def is_called(self, action, term):
        return action.split("/")[-1] == term.split("/")[-1]

    def get_instance_id(self, topic=None):
        if topic is not None:
            return topic.split("/")[2]
        if equipment_key in self._metadata:
            return self._metadata[equipment_key]["instance_id"]
        
    def get_equipment_id(self):
        if equipment_key in self._metadata:
            return self._metadata[equipment_key]["equipment_id"]
        
    def __getattr__(self, item):
        """Dynamically handle attribute access based on equipment terms."""
        if hasattr(self.equipment_terms, item):
            return getattr(self.equipment_terms, item)
        raise AttributeError(f"'MetadataManager' object has no attribute '{item}'")


class EquipmentTerms:
    def __init__(self, dictionary, metadata):
        """
        Initialize EquipmentTerms with YAML dictionary and metadata.
        The metadata dictionary is used to dynamically replace
        placeholders like <institute>.
        """
        self._metadata = metadata
        for key, value in dictionary.items():
            if isinstance(value, dict):
                setattr(self, key, EquipmentTerms(value, metadata))
            else:
                setattr(self, key, self._create_function(value))

    def _create_function(self, path_template):
        """
        Create a function that replaces placeholders with metadata values.
        The placeholders like <institute> are dynamically replaced with the values
        from the metadata dictionary, but can be overridden by arguments.
        """

        def replace_placeholders(**kwargs):
            return re.sub(
                r"<([^>]+)>",
                lambda match: kwargs.get(
                    match.group(1), self._get_metadata_value(match.group(1))
                ),
                path_template,
            )

        return replace_placeholders

    def _get_metadata_value(self, key):
        """
        Get the metadata value for a given key.
        The key should correspond directly to the
        structure in metadata, e.g., <institute>
        should map to self._metadata["institute"].
        """
        try:
            return str(self._metadata[equipment_key][key])
        except KeyError:
            return f"+"

    def __repr__(self):
        return f"{self.__dict__}"
