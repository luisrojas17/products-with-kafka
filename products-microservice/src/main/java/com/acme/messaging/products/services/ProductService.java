package com.acme.messaging.products.services;

import com.acme.messaging.products.rest.dto.ProductDto;

public interface ProductService {
	
	String create(ProductDto productRestModel) throws Exception ;

}
