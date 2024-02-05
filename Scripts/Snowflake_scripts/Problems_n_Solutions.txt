-- Problems to solve using Otodom dataset:


1) What is the average rental price of 1 room, 2 room, 3 room and 4 room apartments in some of the major cities in Poland? 
	Arrange the result such that avg rent for each type fo room is shown in seperate column

	select city, round(avg_rent_1R,2) as avg_rent_1R
	, round(avg_rent_2R,2) as avg_rent_2R, round(avg_rent_3R,2) as avg_rent_3R
	, round(avg_rent_4R,2) as avg_rent_4R
	from (
	    select city,no_of_rooms,price_new
	    from otodom_data_transformed
	    where city in ('Warszawa', 'Wrocław', 'Kraków', 'Gdańsk', 'Katowice', 'Łódź')
	    and apartment_flag = 'apartment'
	    and is_for_sale='false' 
	    and no_of_rooms in (1,2,3,4)) x
	pivot 
	    (
	        avg(price_new)
	        for no_of_rooms in ('1','2','3','4')
	    ) 
	    as p(city,avg_rent_1R, avg_rent_2R, avg_rent_3R, avg_rent_4R)
	order by avg_rent_4R desc 




2) I want to buy an apartment which is around 90-100 m2 and within a range of 800,000 to 1M, display the suburbs in warsaw where I can find such apartments.
	
	select suburb, count(1), max(price_new) max_price, min(price_new) min_price
	from otodom_data_transformed
	where city in ('Warszawa')
	and apartment_flag = 'apartment'
	and is_for_sale = 'true'
	and surface_new between 90 and 100
	and price_new between 800000 and 1000000
	group by suburb
	order by count(1)  desc;




3) What size of an apartment can I expect with a monthly rent of 3000 to 4000 PLN in different major cities of Poland?

	select city, avg(surface_new) avg_area
	from otodom_data_transformed
	where city in ('Warszawa', 'Wrocław', 'Kraków', 'Gdańsk', 'Katowice', 'Łódź')
	and apartment_flag = 'apartment'
	and is_for_sale = 'false'
	and price_new between 3000 and 4000
	group by city
	order by avg_area;	




4) What are the most expensive apartments in major cities of Poland? Display the ad title in english along with city, suburb, cost, size.

	with cte as
	(select city, max(price_new) max_price, min(price_new) min_price
	from otodom_data_transformed
	where city in ('Warszawa', 'Wrocław', 'Kraków', 'Gdańsk', 'Katowice', 'Łódź')
	and apartment_flag = 'apartment'
	and is_for_sale = 'true'
	group by city)
	select x.rn, x.title_eng, x.city, x.suburb, x.price_new, x.surface_new, x.url
	from otodom_data_transformed x
	join cte on cte.city=x.city and cte.max_price=x.price_new
	where apartment_flag = 'apartment'
	and is_for_sale = 'true'
	order by x.city,x.price_new;	




5) What is the percentage of private & business ads on otodom?

	with all_ads as
	(select count(1) tot_ads from otodom_data_transformed),
	ads_type as
	(select advertiser_type
	, sum(case when advertiser_type='business' then 1 end) as business_ads
	, sum(case when advertiser_type='private' then 1 end) as private_ads
	from otodom_data_transformed
	group by advertiser_type)
	select concat(round((max(business_ads) * 100)/max(tot_ads),2),'%') as business_ads_perc
	, concat(round((max(private_ads) * 100)/max(tot_ads),2),'%') as private_ads_perc
	from ads_type ty
	cross join all_ads al ;




6) What is the avg sale price for 3 room apartments within 50-70 m2 area in major cities of Poland?

	select city, round(avg(price_new),2) as avg_sale_price
	from otodom_data_transformed
	where city in ('Warszawa', 'Wrocław', 'Kraków', 'Gdańsk', 'Katowice', 'Łódź')
	and apartment_flag = 'apartment'
	and is_for_sale = 'true'
    and no_of_rooms = 3
	and surface_new between 50 and 70
	group by city
	order by avg_sale_price desc;




7) What is the average rental price for apartments in warsaw in different suburbs?
	Categorize the result based on surface area 0-50, 50-100 and over 100.

	with cte1 as
	(select a.*
	, case when surface_new between 0 and 50 then '0-50'
	when surface_new between 50 and 100 then '50-100'
	when surface_new > 100 then '>100'
	end as area_category
	from otodom_data_transformed a
	where city = 'Warszawa'
	and apartment_flag = 'apartment'
	and is_for_sale = 'false'
	and suburb is not null ),
	cte2 as
	(select suburb
	, case when area_category = '0-50' then avg(price_new) end as avg_price_upto50
	, case when area_category = '50-100' then avg(price_new) end as avg_price_upto100
	, case when area_category = '>100' then avg(price_new) end as avg_price_over100
	from cte1
	group by suburb,area_category)
	select suburb
	, round(max(avg_price_upto50),2) as avg_price_upto_50
	, round(max(avg_price_upto100),2) as avg_price_upto_100
	, round(max(avg_price_over100),2) as avg_price_over_100
	from cte2
	group by suburb
	order by suburb;




8) Which are the top 3 most luxurious neighborhoods in Warsaw? Luxurious neighborhoods can be defined as suburbs which has the most no of of apartments costing over 2M in cost.

	select suburb, luxurious_apartments
	from (
	select suburb, count(1) luxurious_apartments
	, rank() over(order by luxurious_apartments desc ) as rn
	from otodom_data_transformed
	where city = 'Warszawa'
	and apartment_flag = 'apartment'
	and is_for_sale = 'true'
	and price_new > 2000000
	and suburb is not null
	group by suburb) x
	where x.rn <= 3;	




9) Most small families would be looking for apartment with 40-60 m2 in size. Identify the top 5 most affordable neighborhoods in warsaw.

	select suburb, avg_price, no_of_apartments
	from (
	select suburb, round(avg(price_new),2) avg_price, count(1) as no_of_apartments
	, rank() over(order by avg_price ) as rn
	from otodom_data_transformed
	where city = 'Warszawa'
	and apartment_flag = 'apartment'
	and is_for_sale = 'false'
	and surface_new between 40 and 60
	and suburb is not null
	group by suburb) x
	where x.rn <= 5;




10) Which suburb in warsaw has the most and least no of private ads?

	select distinct
	first_value(suburb||' - '||count(1)) over(order by count(1)) as least_private_ads
	, last_value(suburb||' - '||count(1)) over(order by count(1)) as most_private_ads
	from otodom_data_transformed
	where city = 'Warszawa'
	and advertiser_type = 'private'
	and suburb is not null
	group by suburb;


	

11) What is the average rental price and sale price in some of the major cities in Poland?
	
	with cte as
	(select city
	, (case when is_for_sale='false' then round(avg(price_new),2) end) as avg_rental
	, (case when is_for_sale='true'  then round(avg(price_new),2) end) as avg_sale
	from otodom_data_transformed
	where city in ('Warszawa', 'Wrocław', 'Kraków', 'Gdańsk', 'Katowice', 'Łódź')
	and apartment_flag = 'apartment'
	group by city, is_for_sale)
	select city, max(avg_rental) as avg_rental, max(avg_sale) as avg_sale
	from cte
	group by city
	order by avg_rental desc ;	