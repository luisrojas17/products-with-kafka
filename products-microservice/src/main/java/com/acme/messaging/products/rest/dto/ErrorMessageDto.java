package com.acme.messaging.products.rest.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;

@Getter
@Setter
public class ErrorMessageDto {

	private Date timestamp;
	private String message;
	private String details;
	
	public ErrorMessageDto() {
		
	}

	public ErrorMessageDto(Date timestamp, String message, String details) {
		this.timestamp = timestamp;
		this.message = message;
		this.details = details;
	}

}
