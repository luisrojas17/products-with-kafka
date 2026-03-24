package com.acme.messaging.notifications.repositories;

import com.acme.messaging.notifications.repositories.entities.ProcessedEventEntity;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProcessedEventRepository extends JpaRepository<ProcessedEventEntity, Long> {
	
	ProcessedEventEntity findByMessageId(String messageId);

}
