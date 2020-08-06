create database if not exists metastore;
use metastore;

CREATE TABLE `tenant_lookup` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `instance_name` varchar(255) NOT NULL,
  `tenant_name` varchar(45) NOT NULL,
  `db_names` varchar(1024) NOT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `instance_name` (`instance_name`,`tenant_name`)
) ENGINE=InnoDB AUTO_INCREMENT=171 DEFAULT CHARSET=utf8;

CREATE TABLE `schema_refresh` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `maxwell_instance_name` varchar(128) DEFAULT NULL,
  `restart` tinyint(1) DEFAULT NULL,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_name` (`maxwell_instance_name`)
) ENGINE=InnoDB AUTO_INCREMENT=530 DEFAULT CHARSET=utf8;

CREATE TABLE `org_tenant` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `org` varchar(45) NOT NULL,
  `tenant` varchar(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `org_tenant_unique` (`org`,`tenant`)
) ENGINE=InnoDB AUTO_INCREMENT=158 DEFAULT CHARSET=utf8;

CREATE TABLE `entity` (
  `entity_id` int(11) NOT NULL AUTO_INCREMENT,
  `org_tenant_id` int(11) NOT NULL,
  `namespace` varchar(128) DEFAULT NULL,
  `object_name` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`entity_id`),
  UNIQUE KEY `org_UNIQUE` (`org_tenant_id`,`namespace`,`object_name`),
  CONSTRAINT `entity_ibfk_1` FOREIGN KEY (`org_tenant_id`) REFERENCES `org_tenant` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=7494 DEFAULT CHARSET=utf8;

CREATE TABLE `topic_mapping` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `entity_id` int(11) NOT NULL,
  `topic_name` varchar(180) DEFAULT NULL,
  `num_partitions` mediumint(9) NOT NULL,
  `replication_factor` mediumint(9) NOT NULL,
  `retention_hours` mediumint(9) NOT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `entity_id_UNIQUE` (`entity_id`),
  UNIQUE KEY `topic_name_UNIQUE` (`topic_name`),
  KEY `topic_mapping_index` (`id`,`created_at`,`updated_at`),
  CONSTRAINT `topic_mapping_ibfk_1` FOREIGN KEY (`entity_id`) REFERENCES `entity` (`entity_id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=5830 DEFAULT CHARSET=utf8;

CREATE TABLE `schema_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `schema_type` varchar(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `schema_type_UNIQUE` (`schema_type`),
  KEY `schema_type_index` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=4 DEFAULT CHARSET=utf8;

CREATE TABLE `action_type` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `action_type` varchar(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `action_type_UNIQUE` (`action_type`)
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8;

CREATE TABLE `source_db` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `name` varchar(45) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `source_db_UNIQUE` (`name`)
) ENGINE=InnoDB AUTO_INCREMENT=10 DEFAULT CHARSET=utf8;

CREATE TABLE `schema_details` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `entity_id` int(11) NOT NULL,
  `schema_type_id` int(11) DEFAULT '1',
  `action_type_id` int(11) NOT NULL,
  `source_db_id` int(11) NOT NULL,
  `is_active` tinyint(1) DEFAULT NULL,
  `created_at` datetime DEFAULT CURRENT_TIMESTAMP,
  `created_by` varchar(128) DEFAULT NULL,
  `updated_at` datetime DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  `updated_by` varchar(128) DEFAULT NULL,
  `primary_key` varchar(128) DEFAULT NULL,
  `columns` text,
  `schema_version` varchar(255) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `entity_id_UNIQUE` (`entity_id`,`schema_version`),
  KEY `schema_type_id` (`schema_type_id`),
  KEY `action_type_id` (`action_type_id`),
  KEY `source_db_id` (`source_db_id`),
  CONSTRAINT `schema_details_ibfk_1` FOREIGN KEY (`schema_type_id`) REFERENCES `schema_type` (`id`) ON DELETE CASCADE,
  CONSTRAINT `schema_details_ibfk_2` FOREIGN KEY (`entity_id`) REFERENCES `entity` (`entity_id`) ON DELETE CASCADE,
  CONSTRAINT `schema_details_ibfk_3` FOREIGN KEY (`action_type_id`) REFERENCES `action_type` (`id`) ON DELETE CASCADE,
  CONSTRAINT `schema_details_ibfk_4` FOREIGN KEY (`source_db_id`) REFERENCES `source_db` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=32066 DEFAULT CHARSET=utf8;

insert into org_tenant values(1,'ola', 'localhost');
insert into schema_type values (1,'table') , (2,'fact') , (3, 'EVENT');
insert into entity values (1,1,'test','events');
insert into action_type values (1,'RAW_ONLY');
insert into source_db values(1,'MYSQL');


